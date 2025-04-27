package api

import (
	"nanokv-go/api/packet"

	flatbuffers "github.com/google/flatbuffers/go"
)

type WatchRequest struct {
	inner *packet.Watch
}

func ParseWatch(buf []byte) (result WatchRequest) {
	pkt := packet.GetRootAsWatch(buf, 0)
	return WatchRequest{inner: pkt}
}

func (watch *WatchRequest) Id() int32 {
	return watch.inner.Id()
}

func (watch *WatchRequest) Keys(yield func(int, []byte) bool) {
	for i := range watch.inner.KeysLength() {
		var key packet.WatchKey
		if watch.inner.Keys(&key, i) {
			if !yield(int(i), key.KeyBytes()) {
				return
			}
		}
	}
}

var emptyOutput = func() []byte {
	builder := flatbuffers.NewBuilder(64)
	packet.WatchOutputStart(builder)
	packet.WatchOutputAddId(builder, 0)
	builder.Finish(packet.WatchOutputEnd(builder))
	return builder.FinishedBytes()
}()

func (watch *WatchRequest) EmptyOutput() []byte {
	return emptyOutput
}

func (watch *WatchRequest) BuildFirstOutput(cb func(key []byte) *ValueHolder) []byte {
	builder := flatbuffers.NewBuilder(4096)
	l := watch.inner.KeysLength()
	entriesOffsets := make([]flatbuffers.UOffsetT, l)
	for i, key := range watch.Keys {
		value := cb(key)
		keyoff := builder.CreateByteVector(key)
		var valueoff flatbuffers.UOffsetT
		if value != nil {
			valueoff = builder.CreateByteVector(value.Value)
		}
		packet.KvEntryStart(builder)
		packet.KvEntryAddKey(builder, keyoff)
		if value != nil {
			packet.KvEntryAddValue(builder, valueoff)
			packet.KvEntryAddEncoding(builder, value.Encoding)
			packet.KvEntryAddVersionstamp(builder, value.Versionstamp)
		}
		entriesOffsets[i] = packet.KvEntryEnd(builder)
	}
	values := builder.CreateVectorOfTables(entriesOffsets)
	packet.WatchOutputStart(builder)
	packet.WatchOutputAddId(builder, watch.inner.Id())
	packet.WatchOutputAddValues(builder, values)
	builder.Finish(packet.WatchOutputEnd(builder))
	return builder.FinishedBytes()
}

func BuildWatchOutput(events map[string]*ValueHolder) []byte {
	builder := flatbuffers.NewBuilder(4096)
	entriesOffsets := make([]flatbuffers.UOffsetT, 0)
	for key, value := range events {
		keyoff := builder.CreateByteVector([]byte(key))
		var valueoff flatbuffers.UOffsetT
		if value != nil {
			valueoff = builder.CreateByteVector(value.Value)
		}
		packet.KvEntryStart(builder)
		packet.KvEntryAddKey(builder, keyoff)
		if value != nil {
			packet.KvEntryAddValue(builder, valueoff)
			packet.KvEntryAddEncoding(builder, value.Encoding)
			packet.KvEntryAddVersionstamp(builder, value.Versionstamp)
		}
		entriesOffsets = append(entriesOffsets, packet.KvEntryEnd(builder))
	}
	values := builder.CreateVectorOfTables(entriesOffsets)
	packet.WatchOutputStart(builder)
	packet.WatchOutputAddId(builder, 0)
	packet.WatchOutputAddValues(builder, values)
	builder.Finish(packet.WatchOutputEnd(builder))
	return builder.FinishedBytes()
}
