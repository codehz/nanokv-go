package api

import (
	"nanokv-go/api/packet"

	flatbuffers "github.com/google/flatbuffers/go"
)

type AtomicWriteHandler struct {
	*packet.AtomicWrite
}

func ParseAtomicWrite(buf []byte) AtomicWriteHandler {
	return AtomicWriteHandler{packet.GetRootAsAtomicWrite(buf, 0)}
}

func (a AtomicWriteHandler) ForEachCheck(cb func(key []byte, versionstamp uint64) bool) bool {
	for i := range a.ChecksLength() {
		var check packet.Check
		if a.Checks(&check, i) {
			ok := cb(check.KeyBytes(), check.Versionstamp())
			if !ok {
				return false
			}
		} else {
			panic("unreachable")
		}
	}
	return true
}

func (a AtomicWriteHandler) ForEachMutation(cb func(mtype packet.MutationType, key []byte, value []byte, encoding byte, expiresAt uint64) error) error {
	for i := range a.MutationsLength() {
		var mutation packet.Mutation
		if a.Mutations(&mutation, i) {
			err := cb(
				mutation.Type(),
				mutation.KeyBytes(),
				mutation.ValueBytes(),
				mutation.Encoding(),
				mutation.ExpiresAt(),
			)
			if err != nil {
				return err
			}
		} else {
			panic("unreachable")
		}
	}
	return nil
}

func (a AtomicWriteHandler) ForEachEnqueue(cb func(key []byte, value []byte, encoding byte, schedule uint64) error) error {
	for i := range a.EnqueuesLength() {
		var enqueue packet.Enqueue
		if a.Enqueues(&enqueue, i) {
			err := cb(enqueue.KeyBytes(), enqueue.ValueBytes(), enqueue.Encoding(), enqueue.Schedule())
			if err != nil {
				return err
			}
		} else {
			panic("unreachable")
		}
	}
	return nil
}

func (a AtomicWriteHandler) ForEachDequeue(cb func(key []byte, schedule uint64, sequence uint64) error) error {
	for i := range a.DequeuesLength() {
		var dequeue packet.Dequeue
		if a.Dequeues(&dequeue, i) {
			err := cb(dequeue.KeyBytes(), dequeue.Schedule(), dequeue.Sequence())
			if err != nil {
				return err
			}
		} else {
			panic("unreachable")
		}
	}
	return nil
}

func (a AtomicWriteHandler) Finish(ok bool, versionstamp uint64) []byte {
	builder := flatbuffers.NewBuilder(64)
	packet.AtomicWriteOutputStart(builder)
	packet.AtomicWriteOutputAddOk(builder, ok)
	packet.AtomicWriteOutputAddVersionstamp(builder, versionstamp)
	builder.Finish(packet.AtomicWriteOutputEnd(builder))
	return builder.FinishedBytes()
}
