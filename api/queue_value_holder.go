package api

import (
	"errors"
)

type QueueValueHolder struct {
	Value    []byte
	Encoding byte
}

func (holder QueueValueHolder) Encode() (result []byte) {
	result = make([]byte, len(holder.Value)+1)
	result[0] = holder.Encoding
	copy(result[1:], holder.Value)
	return
}

func (holder *QueueValueHolder) Decode(data []byte) {
	if len(data) < 1 {
		panic(errors.New("invalid data"))
	}
	holder.Value = data[1:]
	holder.Encoding = data[0]
}

type QueueEntry struct {
	Key      []byte
	Value    []byte
	Encoding byte
	Schedule uint64
	Sequence uint64
}
