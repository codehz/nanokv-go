package api

import (
	"nanokv-go/api/packet"

	flatbuffers "github.com/google/flatbuffers/go"
)

type SnapshotReadHandler struct {
	*packet.SnapshotRead
	builder *flatbuffers.Builder
}

type KvEntryOutput struct {
	key          flatbuffers.UOffsetT
	value        flatbuffers.UOffsetT
	encoding     byte
	versionstamp uint64
}

func ParseSnapshotRead(buf []byte) SnapshotReadHandler {
	return SnapshotReadHandler{packet.GetRootAsSnapshotRead(buf, 0), flatbuffers.NewBuilder(4096)}
}

func (s SnapshotReadHandler) CreateByteVector(value []byte) flatbuffers.UOffsetT {
	return s.builder.CreateByteVector(value)
}

func (s SnapshotReadHandler) CreateKvEntry(key []byte, holder *ValueHolder) (output KvEntryOutput) {
	return KvEntryOutput{
		key:          s.CreateByteVector(key),
		value:        s.CreateByteVector(holder.Value),
		encoding:     holder.Encoding,
		versionstamp: holder.Versionstamp,
	}
}

func (s SnapshotReadHandler) Transform(cb func(start []byte, end []byte, limit uint32, exact bool, reverse bool) ([]KvEntryOutput, error)) (out []byte, err error) {
	l := s.RequestsLength()
	ranges := make([]flatbuffers.UOffsetT, l)
	for i := range l {
		var readRange packet.ReadRange
		if s.Requests(&readRange, i) {
			var entries []KvEntryOutput
			var entriesOffsets []flatbuffers.UOffsetT
			entries, err = cb(readRange.StartBytes(), readRange.EndBytes(), readRange.Limit(), readRange.Exact(), readRange.Reverse())
			if err != nil {
				return
			}
			for _, entry := range entries {
				packet.KvEntryStart(s.builder)
				packet.KvEntryAddKey(s.builder, entry.key)
				packet.KvEntryAddValue(s.builder, entry.value)
				packet.KvEntryAddEncoding(s.builder, entry.encoding)
				packet.KvEntryAddVersionstamp(s.builder, entry.versionstamp)
				entriesOffsets = append(entriesOffsets, packet.KvEntryEnd(s.builder))
			}
			valuesOffset := s.builder.CreateVectorOfTables(entriesOffsets)
			packet.ReadRangeOutputStart(s.builder)
			packet.ReadRangeOutputAddValues(s.builder, valuesOffset)
			ranges[i] = packet.ReadRangeOutputEnd(s.builder)
		} else {
			panic("unreachable")
		}
	}
	rangesOffset := s.builder.CreateVectorOfTables(ranges)
	packet.SnapshotReadOutputStart(s.builder)
	packet.SnapshotReadOutputAddRanges(s.builder, rangesOffset)
	s.builder.Finish(packet.SnapshotReadOutputEnd(s.builder))
	out = s.builder.FinishedBytes()
	return
}
