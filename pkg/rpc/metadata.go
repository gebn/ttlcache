package rpc

import (
	"bytes"
	"encoding/gob"
)

// Metadata contains additional information about a given peer, and is required
// for other peers to be able to interact with it. Direct use of this struct is
// only necessary if already using memberlist's metadata facility elsewhere, and
// you want to embed this package's metadata in your own. Otherwise, simply use
// NewDelegate() passing in the port directly.
type Metadata struct {

	// Port is the port the node is listening for peer loads on.
	Port uint16
}

// Encode serialises the metadata into a byte slice for passing to memberlist.
// The bytes should not be interpreted by the caller; we may switch to protobuf
// in future.
func (m *Metadata) Encode() []byte {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(m); err != nil {
		// can only be caused by OOM - rather than making the caller attempt to
		// deal with this, just fail loudly
		panic(err)
	}
	return buf.Bytes()
}

// Decode unserialises the metadata received from a peer, generated by Encode().
func (m *Metadata) Decode(b []byte) error {
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)
	if err := dec.Decode(m); err != nil {
		return err
	}
	return nil
}
