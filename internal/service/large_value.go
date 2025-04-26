package service

import "encoding/binary"

func (srv *Service) storeLargeValue(value []byte) (result []byte) {
	kv, handle := srv.db.NewKV()
	for i := range len(value)/65536 + 1 {
		kv.Set(binary.BigEndian.AppendUint16(nil, uint16(i)), value[i*65536:min(len(value), (i+1)*65536)])
	}
	result = make([]byte, 16)
	binary.LittleEndian.PutUint64(result[0:], uint64(handle))
	binary.LittleEndian.PutUint64(result[8:], uint64(len(value)))
	return
}

func (srv *Service) removeLargeValue(value []byte) {
	srv.db.RemoveKVByHandle(int64(binary.LittleEndian.Uint64(value)))
}

func (srv *Service) loadLargeValue(value []byte) (result []byte) {
	kv := srv.db.GetKV(int64(binary.LittleEndian.Uint64(value)))
	result = make([]byte, 0, binary.LittleEndian.Uint64(value[8:]))
	for _, chunk := range kv.All {
		result = append(result, chunk...)
	}
	return
}
