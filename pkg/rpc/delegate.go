package rpc

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
	return newDelegate(m.Encode())
}

func newDelegate(meta []byte) Delegate {
	return Delegate{
		meta: meta,
	}
}

func (d Delegate) NodeMeta(limit int) []byte {
	return d.meta
}

func (d Delegate) NotifyMsg(_ []byte) {}
func (d Delegate) GetBroadcasts(_, _ int) [][]byte {
	return nil
}
func (d Delegate) LocalState(_ bool) []byte {
	return nil
}
func (d Delegate) MergeRemoteState(_ []byte, _ bool) {}
