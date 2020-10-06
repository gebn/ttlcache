// Package cluster implements peer tracking and selection via memberlist.
package cluster

import (
	"sync/atomic"

	"github.com/gebn/ttlcache/internal/pkg/consistenthash"

	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Literally only PeerPicker links this package to ttlcache.

var (
	peerJoins = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "peer_joins_total",
			Help:      "The number of times we have been notified of a peer joining a memberlist, by cluster name.",
		},
		[]string{"cluster"}, // "group" may be confused with groupcache
	)
	peerLeaves = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "peer_leaves_total",
			Help:      "The number of times we have been notified of a peer leaving a memberlist, by cluster name.",
		},
		[]string{"cluster"},
	)
	// current peers = joins - leaves
	// peer updates = joins + leaves
	ownerRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "owner_requests_total",
			Help:      "The number of times a key's owner has been requested, by cluster name.",
		},
		[]string{"cluster"},
	)
)

// state is an immutable snapshot of members in the cluster. We use a
// copy-on-write strategy to update the ring and peer map, as we expect vastly
// more owner requests than peer updates, and this saves locking.
type state struct {
	Ring  *consistenthash.Ring
	Peers map[string]*memberlist.Node
}

// Tracker implements memberlist.EventDelegate, combining this with a consistent
// hash which is kept up to date with members. Owner lookups query the
// consistent hash. A given tracker is logically tied to a memberlist, not a
// cache. Two separate caches could share the same tracker, e.g. if each had the
// same peers but distinct keys and values.
//
// This has to exist to create a memberlist object, so cannot rely on one. The
// Members() function of memberlist is a bit of a decoy; there are race
// conditions everywhere, but we want to be as consistent with the local
// memberlist as possible without being inefficient, and that means being
// event-driven. Efficiency of updates is sacrificed for efficiency of owner
// lookups.
type Tracker struct {
	// state contains the latest consistent hash and peer map. Note that
	// Notify*() methods will not be called concurrently; we only need to ensure
	// Owner() has read-only access to a reasonably up-to-date consistent view
	// of state.
	state                                atomic.Value
	replicas                             int
	peerJoins, peerLeaves, ownerRequests prometheus.Counter
}

// NewTracker initialises a new tracker with a name and a number of times to
// replicate each node (vnodes) in the internal consistent hash ring. Note
// trackers are not necessarily 1:1 with caches, so this should be the name of
// the local memberlist rather than the cache that will be associated with this
// tracker.
func NewTracker(name string, replicas int) *Tracker {
	tracker := &Tracker{
		replicas:      replicas,
		peerJoins:     peerJoins.WithLabelValues(name),
		peerLeaves:    peerLeaves.WithLabelValues(name),
		ownerRequests: ownerRequests.WithLabelValues(name),
	}
	tracker.updateState(nil)
	return tracker
}

// NotifyJoin is called by memberlist when a peer joins the cluster. We expect a
// flurry of such events during initialisation.
func (t *Tracker) NotifyJoin(node *memberlist.Node) {
	t.peerJoins.Inc()
	state := t.state.Load().(*state)
	peers := make(map[string]*memberlist.Node, len(state.Peers)+1)
	for name, node := range state.Peers {
		peers[name] = node
	}
	peers[node.Name] = node
	t.updateState(peers)
}

func (t *Tracker) NotifyLeave(node *memberlist.Node) {
	t.peerLeaves.Inc()
	state := t.state.Load().(*state)
	peers := make(map[string]*memberlist.Node, len(state.Peers)-1)
	for name, node := range state.Peers {
		peers[name] = node
	}
	delete(peers, node.Name)
	t.updateState(peers)
}

func (t *Tracker) NotifyUpdate(_ *memberlist.Node) {}

func (t *Tracker) updateState(peers map[string]*memberlist.Node) {
	if len(peers) == 0 {
		// initialisation, or all peers left
		t.state.Store(&state{})
		return
	}
	names := make([]string, 0, len(peers))
	for name := range peers {
		names = append(names, name)
	}
	// we ensure len(names) > 0 above, but a nil ring is actually ok regardless
	ring, _ := consistenthash.NewRing(t.replicas, names...)
	t.state.Store(&state{
		Ring:  ring,
		Peers: peers,
	})
}

// Owner uses a consistent hash to find the node responsible for a given key.
// It returns that node, or nil if there are no members in the cluster.
func (t *Tracker) Owner(key string) *memberlist.Node {
	t.ownerRequests.Inc()
	state := t.state.Load().(*state)
	if state.Ring == nil {
		// no owner as no members
		return nil
	}
	return state.Peers[state.Ring.Get(key)]
}
