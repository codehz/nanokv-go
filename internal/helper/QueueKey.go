package helper

import (
	"encoding/binary"
)

type QueueKey struct {
	Key      []byte
	Schedule uint64
	Sequence uint64
}

func (key QueueKey) Encode() (result []byte) {
	l := len(key.Key)
	result = make([]byte, 16+l)
	copy(result, key.Key)
	binary.BigEndian.PutUint64(result[l+0:], key.Schedule)
	binary.BigEndian.PutUint64(result[l+8:], key.Sequence)
	return
}

func (key *QueueKey) Decode(buffer []byte) {
	l := len(buffer) - 16
	key.Key = buffer[0:l]
	key.Schedule = binary.BigEndian.Uint64(buffer[l+0:])
	key.Sequence = binary.BigEndian.Uint64(buffer[l+8:])
}
