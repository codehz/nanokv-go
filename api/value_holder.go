package api

import (
	"encoding/binary"
	"errors"
)

type ValueHolder struct {
	Value        []byte
	Encoding     byte
	Versionstamp uint64
	ExpiresAt    uint64
}

func (holder ValueHolder) Encode() (result []byte) {
	result = make([]byte, len(holder.Value)+17)
	binary.LittleEndian.PutUint64(result[0:], holder.Versionstamp)
	binary.LittleEndian.PutUint64(result[8:], holder.ExpiresAt)
	result[16] = holder.Encoding
	copy(result[17:], holder.Value)
	return
}

func (holder *ValueHolder) Decode(data []byte) {
	if len(data) < 17 {
		panic(errors.New("invalid data"))
	}
	holder.Value = data[17:]
	holder.Encoding = data[16]
	holder.Versionstamp = binary.LittleEndian.Uint64(data[0:])
	holder.ExpiresAt = binary.LittleEndian.Uint64(data[8:])
}
