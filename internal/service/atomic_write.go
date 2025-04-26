package service

import (
	"encoding/binary"
	"nanokv-go/api"
	"nanokv-go/api/packet"
	"nanokv-go/internal/helper"
	"slices"
	"time"
)

func (srv *Service) HandleAtomicWrite(input []byte) ([]byte, error) {
	atomic := srv.db.Atomic()
	defer atomic.Cancel()
	buf := make([]byte, 1041)
	pkt := api.ParseAtomicWrite(input)
	ok := pkt.ForEachCheck(func(key []byte, versionstamp uint64) (result bool) {
		raw := srv.main.Get(buf, key)
		if raw == nil {
			return versionstamp == 0
		}
		var value api.ValueHolder
		value.Decode(raw)
		return value.Versionstamp == versionstamp
	})
	if !ok {
		return pkt.Finish(false, 0), nil
	}
	versionRaw := srv.db.Get(buf, []byte("global_version"))
	var lastversion uint64
	if versionRaw != nil {
		lastversion = binary.LittleEndian.Uint64(versionRaw)
	}
	nextversion := lastversion + 1
	srv.db.Set([]byte("global_version"), binary.LittleEndian.AppendUint64(nil, nextversion))
	kvevents := make(map[string]*api.ValueHolder)
	var minExpires uint64
	now := uint64(time.Now().UnixMilli())
	err := pkt.ForEachMutation(func(mtype packet.MutationType, key, value []byte, encoding byte, expiresAt uint64) error {
		removeOldValue := func() {
			oldraw := srv.main.Extract(buf, key)
			if oldraw != nil {
				var old api.ValueHolder
				old.Decode(oldraw)
				if old.ExpiresAt > 0 {
					srv.expires.Delete(helper.ExpiresKey{
						ExpiresAt:    old.ExpiresAt,
						Versionstamp: old.Versionstamp,
						Key:          key,
					}.Encode())
				}
				kvevents[string(key)] = nil
			}
		}

		switch mtype {
		case packet.MutationTypeDELETE:
			removeOldValue()
			return nil
		case packet.MutationTypeSET:
			removeOldValue()
			if expiresAt > 0 {
				if now >= expiresAt {
					return nil
				}
				if minExpires == 0 || minExpires > expiresAt {
					minExpires = expiresAt
				}
				srv.expires.Set(helper.ExpiresKey{
					ExpiresAt:    expiresAt,
					Versionstamp: nextversion,
					Key:          key,
				}.Encode(), nil)
			}
			holder := &api.ValueHolder{
				Value:        slices.Clone(value),
				Encoding:     encoding,
				Versionstamp: nextversion,
				ExpiresAt:    expiresAt,
			}
			kvevents[string(key)] = holder
			srv.main.Set(key, holder.Encode())
			return nil
		default:
			return ErrInvalidMutation
		}
	})
	if err != nil {
		return nil, err
	}
	seqbuf := make([]byte, 8)
	minSchedule := uint64(0)
	queueevents := make(map[string][]api.QueueEntry, 0)
	err = pkt.ForEachEnqueue(func(key, value []byte, encoding byte, schedule uint64) error {
		seq := uint64(0)
		_, _, err := srv.sequence.Put(buf, key, func(key, old []byte) (new []byte, write bool, err error) {
			if old != nil {
				oldseq := binary.LittleEndian.Uint64(old)
				seq = oldseq + 1
			}
			binary.LittleEndian.PutUint64(seqbuf, seq)
			return seqbuf, true, nil
		})
		if err != nil {
			return err
		}
		encodedKey := helper.QueueKey{Key: key, Schedule: schedule, Sequence: seq}.Encode()
		encodedValue := api.QueueValueHolder{Value: value, Encoding: encoding}.Encode()
		srv.queue.Set(encodedKey, encodedValue)
		if schedule > now {
			scheduleKey := helper.ScheduleKey{Schedule: schedule, Key: key, Sequence: seq}.Encode()
			srv.schedule.Set(scheduleKey, nil)
			if minSchedule == 0 || minSchedule > schedule {
				minSchedule = schedule
			}
		} else {
			queueevents[string(key)] = append(queueevents[string(key)], api.QueueEntry{
				Key:      key,
				Value:    value,
				Encoding: encoding,
				Schedule: schedule,
				Sequence: seq,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = pkt.ForEachDequeue(func(key []byte, schedule, sequence uint64) error {
		encodedKey := helper.QueueKey{Key: key, Schedule: schedule, Sequence: sequence}.Encode()
		srv.queue.Delete(encodedKey)
		return nil
	})
	if err != nil {
		return nil, err
	}

	atomic.Submit()

	srv.expiresAlarm.Schedule(minExpires)
	srv.scheduleAlarm.Schedule(minSchedule)
	result := pkt.Finish(true, nextversion)
	srv.watch.Emit(kvevents)
	srv.listen.Emit(queueevents)
	return result, nil
}
