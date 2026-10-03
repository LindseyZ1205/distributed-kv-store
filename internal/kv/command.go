package kv

import (
	"encoding/binary"
	"errors"
)

// Op is the kind of write a Command performs.
type Op byte

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

// Command is a write as it travels through the Raft log. ClientID and Seq
// let the state machine apply each write at most once when a client
// retries it.
type Command struct {
	Op       Op
	Key      string
	Value    []byte
	ClientID uint64
	Seq      uint64
}

// encode lays the command out as
//
//	op | uvarint client id | uvarint seq | uvarint len(key) | key | value
//
// The value runs to the end, so it needs no length.
func (c Command) encode() []byte {
	b := make([]byte, 0, 1+4*binary.MaxVarintLen64+len(c.Key)+len(c.Value))
	b = append(b, byte(c.Op))
	b = binary.AppendUvarint(b, c.ClientID)
	b = binary.AppendUvarint(b, c.Seq)
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	return append(b, c.Value...)
}

func decodeCommand(b []byte) (Command, error) {
	if len(b) < 1 {
		return Command{}, errors.New("kv: empty command")
	}
	c := Command{Op: Op(b[0])}
	if c.Op != OpPut && c.Op != OpDelete {
		return Command{}, errors.New("kv: unknown op")
	}
	b = b[1:]
	var fields [3]uint64
	for i := range fields {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return Command{}, errors.New("kv: truncated command")
		}
		fields[i], b = v, b[n:]
	}
	c.ClientID, c.Seq = fields[0], fields[1]
	keyLen := fields[2]
	if keyLen > uint64(len(b)) {
		return Command{}, errors.New("kv: truncated key")
	}
	c.Key = string(b[:keyLen])
	if c.Op == OpPut {
		c.Value = b[keyLen:]
	}
	return c, nil
}
