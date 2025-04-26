package helper

import (
	"encoding/binary"
	"errors"
)

type ExpiresKey struct {
	ExpiresAt    uint64
	Versionstamp uint64
	Key          []byte
}

func (key ExpiresKey) Encode() (result []byte) {
	result = make([]byte, 16+len(key.Key))
	binary.BigEndian.PutUint64(result[0:], key.ExpiresAt)
	binary.BigEndian.PutUint64(result[8:], key.Versionstamp)
	copy(result[16:], key.Key)
	return
}

func (key *ExpiresKey) Decode(buf []byte) {
	if len(buf) < 24 {
		panic(errors.New("invalid expires key"))
	}
	key.ExpiresAt = binary.BigEndian.Uint64(buf[0:])
	key.Versionstamp = binary.BigEndian.Uint64(buf[8:])
	key.Key = buf[16:]
}
