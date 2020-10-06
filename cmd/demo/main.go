package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gebn/ttlcache"
	"github.com/gebn/ttlcache/pkg/cluster"
	"github.com/gebn/ttlcache/pkg/lifetime"
	"github.com/gebn/ttlcache/pkg/rpc"

	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// multiStringValue allows us to accept multiple occurrences of a given flag on
// the command line. Unfortunately this is not built into the flag library.
type multiStringValue []string

func (v *multiStringValue) Set(value string) error {
	*v = append(*v, value)
	return nil
}
func (v *multiStringValue) String() string {
	if v == nil {
		return ""
	}
	return strings.Join(*v, ", ")
}

func load(_ context.Context, key string) ([]byte, lifetime.Lifetime, error) {
	time.Sleep(time.Second * 3)
	return []byte(key + " value"), lifetime.New(time.Hour * 168), nil
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Print("no hostname, using empty string as peer name")
	}
	flgPeerPort := flag.Int("peer.port", 7946, "where to listen for gossip communication")
	flgPeerName := flag.String("peer.name", hostname, "the name of this peer, unique within the cluster")
	flgHTTPPort := flag.Int("http.port", 8080, "used for external requests and metrics")
	flgClusterPeers := &multiStringValue{}
	flag.Var(flgClusterPeers, "cluster.peer", "addr:port of peer to join, including this one; can be passed multiple times")
	flgClusterMinMembers := flag.Int("cluster.min-members", 3, "the minimum number of cluster members before accepting external requests")
	flag.Parse()

	log.Printf("peer.port: %v", *flgPeerPort)
	log.Printf("peer.name: %v", *flgPeerName)
	log.Printf("http.port: %v", *flgHTTPPort)
	log.Printf("cluster.peer: %v", flgClusterPeers)
	log.Printf("cluster.min-members: %v", *flgClusterMinMembers)

	http.Handle("/metrics", promhttp.Handler())
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(*flgHTTPPort))
	if err != nil {
		log.Printf("failed to start web server: %v", err)
		return
	}
	defer listener.Close()

	srv := &http.Server{
		ReadHeaderTimeout: time.Second * 10,
		IdleTimeout:       time.Minute * 3,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(listener); err != http.ErrServerClosed {
			// without the wait group, this line may not be printed in case of
			// failure
			log.Printf("server did not close cleanly: %v", err)
		}
	}()

	tracker := cluster.NewTracker("ttlcache", 50)
	config := memberlist.DefaultLocalConfig()
	config.Name = *flgPeerName
	config.BindPort = *flgPeerPort
	config.AdvertisePort = *flgPeerPort
	config.Delegate = rpc.NewDelegate(uint16(*flgHTTPPort))
	config.Events = tracker
	list, err := memberlist.Create(config)
	if err != nil {
		log.Printf("failed to create memberlist: %v", err)
		return
	}
	// must not exit early from here on
	// tracker is now keeping itself up to date - it's ready for use

	awaitMembers(list, *flgClusterPeers, *flgClusterMinMembers)

	// we have to decide here whether to wait a bit to allow us to add all
	// members, which will ensure we have an up to date view of the world and
	// which nodes are commonly held to be authoritative, or dive straight in
	// and start answering requests. We've opted to wait for the memberlist to
	// stabilise. Requests we receive will immediately 404 due to the handler
	// not being installed yet, however peers will not retry other nodes until
	// the timeout. This is ultimately a toss up between a few origin loads and
	// latency.

	base := ttlcache.NewBase(&ttlcache.BaseOpts{
		Name:                       "symbols",
		PeerPicker:                 cluster.NewPeerPicker(tracker, list),
		AuthoritativeCacheCapacity: 5452,
		HotCacheCapacity:           1_000,
		ParallelRequests:           20,
	})
	pair := &rpc.Pair{
		Prefix:  "/ttlcache/caches/" + base.Name + "/keys/",
		Timeout: time.Second * 5, // origin load + 1
	}
	cache := base.Configure(&ttlcache.ConfigureOpts{
		OriginLoader:      ttlcache.OriginLoaderFunc(load),
		OriginLoadTimeout: time.Second * 4,                        // local, end-to-end, including any retries
		PeerLoader:        pair.Loader(base.Name, &http.Client{}), // must be dedicated client
		PeerLoadTimeout:   time.Second * 10,                       // allow a retry; want to avoid local origin load if possible
		HotAddProbability: 0.05,
	})

	// there is a small window on start during which peers send us requests
	// before our endpoint is up; they will back off
	pair.Handle(http.DefaultServeMux, cache)

	// handler for external (non-peer) requests
	http.Handle("/key/", rpc.UninstrumentedHandler(cache, "/key/", time.Second*5))

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println() // avoids "^C" being printed on the same line as the log date

	// shutdown does not automatically leave - be more graceful
	log.Printf("leaving cluster")
	if err := list.Leave(time.Second * 5); err != nil {
		log.Printf("error leaving cluster: %v", err)
	}
	log.Printf("shutting down memberlist")
	if err := list.Shutdown(); err != nil {
		log.Printf("error shutting down memberlist: %v", err)
	}

	// grace period for the rest of the cluster to realise we've left and stop
	// sending us new requests
	time.Sleep(time.Second * 5)
	log.Printf("waiting for in-progress requests to finish")
	if err := srv.Shutdown(context.Background()); err != nil {
		// either a context or listener error, and it cannot be the former as
		// we're using the background ctx
		log.Printf("failed to close listener: %v", err)
	}
}

// awaitMembers waits for the cluster to reach a certain number of members. In
// the meantime, it keeps trying to join the provided list of peers.
func awaitMembers(list *memberlist.Memberlist, peers []string, threshold int) {
	for {
		// if not using a static list of peers, you'd (re-)fetch an up to date
		// peer list here
		if _, err := list.Join(peers); err != nil {
			log.Printf("node did not successfully join cluster: %v", err)
			time.Sleep(time.Second * 2)
			continue
		}

		// contacted is the number of addresses reached, which may be more than
		// the number of unique peers, e.g. if they are dual stack; it may also
		// be 0 with no error if *flgClusterPeers was empty
		members := list.NumMembers()
		if members >= threshold {
			log.Printf("reached %v members", threshold)
			break
		}
		log.Printf("have %v member(s); waiting for %v before continuing", members, threshold)
		time.Sleep(time.Second)
	}
}
