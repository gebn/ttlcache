package rpc

import (
	"fmt"
)

// Delegate is a trivial memberlist.Delegate implementation that responds with a
// static set of metadata. This struct is immutable.
type Delegate struct {
	meta []byte
}

// NewDelegate returns a delegate that will serve metadata identifying the
// provided port.
func NewDelegate(port uint16) Delegate {
	// it would be more maintainable to take a Metadata struct, however this
	// isn't likely to change any time soon
	m := &Metadata{
		Port: port,
	}
	return Delegate{
		meta: m.Encode(),
	}
}

func (d Delegate) NodeMeta(limit int) []byte {
	if len(d.meta) > limit {
		// memberlist will panic with a less helpful error
		panic(fmt.Sprintf("can return at most %v bytes of meta, have %v", limit, len(d.meta)))
	}
	return d.meta
}

func (d Delegate) NotifyMsg(_ []byte)                {}
func (d Delegate) GetBroadcasts(_, _ int) [][]byte   { return nil }
func (d Delegate) LocalState(_ bool) []byte          { return nil }
func (d Delegate) MergeRemoteState(_ []byte, _ bool) {}
