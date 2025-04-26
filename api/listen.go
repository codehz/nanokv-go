package api

import (
	"iter"
	"nanokv-go/api/packet"

	flatbuffers "github.com/google/flatbuffers/go"
)

type ListenRequest struct {
	inner *packet.Listen
}

func ParseListen(buf []byte) ListenRequest {
	pkt := packet.GetRootAsListen(buf, 0)
	return ListenRequest{inner: pkt}
}

func (listen ListenRequest) Added(yield func(int, []byte) bool) {
	for i := range listen.inner.AddedLength() {
		var key packet.ListenKey
		if listen.inner.Added(&key, i) {
			if !yield(int(i), key.KeyBytes()) {
				return
			}
		}
	}
}

func (listen ListenRequest) Removed(yield func(int, []byte) bool) {
	for i := range listen.inner.RemovedLength() {
		var key packet.ListenKey
		if listen.inner.Removed(&key, i) {
			if !yield(int(i), key.KeyBytes()) {
				return
			}
		}
	}
}

func BuildListenOutput(list iter.Seq[QueueEntry]) []byte {
	builder := flatbuffers.NewBuilder(4096)
	entriesOffsets := make([]flatbuffers.UOffsetT, 0)
	for entry := range list {
		keyoff := builder.CreateByteVector(entry.Key)
		valueoff := builder.CreateByteVector(entry.Value)
		packet.QueueEntryStart(builder)
		packet.QueueEntryAddKey(builder, keyoff)
		packet.QueueEntryAddValue(builder, valueoff)
		packet.QueueEntryAddEncoding(builder, entry.Encoding)
		packet.QueueEntryAddSchedule(builder, entry.Schedule)
		packet.QueueEntryAddSequence(builder, entry.Sequence)
		entriesOffsets = append(entriesOffsets, packet.QueueEntryEnd(builder))
	}
	entries := builder.CreateVectorOfTables(entriesOffsets)
	packet.ListenOutputStart(builder)
	packet.ListenOutputAddEntries(builder, entries)
	builder.Finish(packet.ListenOutputEnd(builder))
	return builder.FinishedBytes()
}
