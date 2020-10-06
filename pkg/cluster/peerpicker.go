package cluster

import (
	"github.com/hashicorp/memberlist"
)

// PeerPicker implements ttlcache.PeerPicker for memberlist.
type PeerPicker struct {
	tracker *Tracker
	ourName string
}

// NewPeerPicker creates an object from a Tracker that follows PeerPicker
// interface rules. This is separate from tracker, as it can only be created
// once the local node is known.
func NewPeerPicker(tracker *Tracker, list *memberlist.Memberlist) PeerPicker {
	return PeerPicker{
		tracker: tracker,
		ourName: list.LocalNode().Name,
	}
}

func (p PeerPicker) PickPeer(key string) *memberlist.Node {
	// identical to Tracker.Owner(), however we must filter out the local node
	peer := p.tracker.Owner(key)
	if peer == nil || peer.Name == p.ourName {
		return nil
	}
	return peer
}
