package service

import (
	"nanokv-go/api"
)

func (srv *Service) HandleSnapshotRead(input []byte) ([]byte, error) {
	view := srv.db.View()
	defer view.Close()
	pkt := api.ParseSnapshotRead(input)
	buf := make([]byte, 1041)
	return pkt.Transform(func(start []byte, end []byte, limit uint32, exact bool, reverse bool) (entries []api.KvEntryOutput, err error) {
		var value api.ValueHolder
		if exact {
			raw := srv.main.Get(buf, start)
			if raw != nil {
				value.Decode(raw)
				if value.LargeValue {
					value.Value = srv.loadLargeValue(value.Value)
				}
				entries = append(entries, pkt.CreateKvEntry(start, &value))
				return entries, nil
			} else {
				return
			}
		}
		if limit == 0 {
			return
		}
		for key, raw := range srv.main.Iter(start, end, reverse) {
			value.Decode(raw)
			if value.LargeValue {
				value.Value = srv.loadLargeValue(value.Value)
			}
			entries = append(entries, pkt.CreateKvEntry(key, &value))
		}
		return
	})
}
