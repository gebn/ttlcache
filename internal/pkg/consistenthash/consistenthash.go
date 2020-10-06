// Package consistenthash provides a ring hash implementation.
package consistenthash

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/cespare/xxhash"
)

var (
	// ErrNoNodes is returned when a hash is requested containing no nodes.
	ErrNoNodes = errors.New(
		"there must be at least one node, otherwise keys cannot be mapped")
)

// uint64Slice implements sort.Interface for []uint64.
type uint64Slice []uint64

func (s uint64Slice) Len() int           { return len(s) }
func (s uint64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Ring is an immutable ring hash.
type Ring struct {

	// vnodes is a sorted slice of hashed vnodes. It contains one entry for each
	// replica of each node.
	vnodes []uint64

	// vnodeNodes maps each element in vnodes back to its original node name,
	// for providing back to the caller of Get().
	vnodeNodes map[uint64]string
}

// NewRing creates a new consistent hash ring, initialised with the provided
// nodes (order does not matter). When the node list changes, a new hash should
// be created; updating a hash in place is not supported, as it would
// drastically increase the time complexity of lookups, which are expected to be
// the vastly more common operation. Replicas must be at least one, and should
// be greater when there are few nodes in order to reduce the variance in load
// amongst them. 50 is usually a reasonable choice.
func NewRing(replicas int, nodes ...string) (*Ring, error) {
	if replicas < 1 {
		return nil, fmt.Errorf(
			"there must be at least one virtual node for each node; %v specified",
			replicas)
	}
	if len(nodes) == 0 {
		return nil, ErrNoNodes
	}
	vnodes := make([]uint64, 0, len(nodes)*replicas)
	vnodeNodes := make(map[uint64]string, len(nodes)*replicas)
	for _, node := range nodes {
		for i := 0; i < replicas; i++ {
			vnode := xxhash.Sum64String(strconv.Itoa(i) + node)
			vnodes = append(vnodes, vnode)
			vnodeNodes[vnode] = node
		}
	}
	sort.Sort(uint64Slice(vnodes))
	return &Ring{
		vnodes:     vnodes,
		vnodeNodes: vnodeNodes,
	}, nil
}

// Get returns the node in the hash that owns the provided key.
func (m *Ring) Get(key string) string {
	hash := xxhash.Sum64String(key)

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.vnodes), func(i int) bool {
		return m.vnodes[i] >= hash
	})

	// we have cycled back to the first replica
	if idx == len(m.vnodes) {
		idx = 0
	}

	return m.vnodeNodes[m.vnodes[idx]]
}
