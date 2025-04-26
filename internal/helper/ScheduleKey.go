package helper

import (
	"encoding/binary"
)

type ScheduleKey struct {
	Schedule uint64
	Key      []byte
	Sequence uint64
}

func (key ScheduleKey) Encode() (result []byte) {
	l := len(key.Key)
	result = make([]byte, l+16)
	binary.BigEndian.PutUint64(result[0:], key.Schedule)
	copy(result[8:], key.Key)
	binary.BigEndian.PutUint64(result[8+l:], key.Sequence)
	return
}

func (key *ScheduleKey) Decode(buffer []byte) {
	l := len(buffer) - 16
	key.Schedule = binary.BigEndian.Uint64(buffer[0:])
	key.Key = buffer[8 : l+8]
	key.Sequence = binary.BigEndian.Uint64(buffer[l+8:])
}
