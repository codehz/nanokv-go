package service

import (
	"nanokv-go/api"
	"nanokv-go/internal/emitter"
	"nanokv-go/internal/group"
	"nanokv-go/internal/helper"
	"nanokv-go/internal/mkv"
	"time"
)

type Service struct {
	db            *mkv.DB
	main          *mkv.KV
	expires       *mkv.KV
	queue         *mkv.KV
	sequence      *mkv.KV
	schedule      *mkv.KV
	watch         *emitter.Emitter[*api.ValueHolder]
	listen        *emitter.Emitter[[]api.QueueEntry]
	expiresAlarm  *helper.Alarm
	scheduleAlarm *helper.Alarm
}

func NewService(db *mkv.DB) (result *Service) {
	atomic := db.Atomic()
	defer atomic.Cancel()
	result = &Service{
		db:            db,
		main:          db.EnsureKV("main"),
		expires:       db.EnsureKV("expires"),
		queue:         db.EnsureKV("queue"),
		sequence:      db.EnsureKV("sequence"),
		schedule:      db.EnsureKV("schedule"),
		watch:         emitter.NewEmitter[*api.ValueHolder](8),
		listen:        emitter.NewEmitter[[]api.QueueEntry](8),
		expiresAlarm:  helper.NewAlarm(),
		scheduleAlarm: helper.NewAlarm(),
	}
	atomic.Submit()
	return
}

func (srv *Service) cleanExpires(now uint64) uint64 {
	atomic := srv.db.Atomic()
	defer atomic.Cancel()

	var expiresKey helper.ExpiresKey
	events := make(map[string]*api.ValueHolder)
	next := uint64(0)
	count := 0
	for key := range srv.expires.All {
		expiresKey.Decode(key)
		if expiresKey.ExpiresAt > now {
			if next > 0 {
				next = min(next, expiresKey.ExpiresAt)
			} else {
				next = expiresKey.ExpiresAt
			}
			break
		}
		srv.main.Delete(expiresKey.Key)
		srv.expires.Delete(key)
		events[string(expiresKey.Key)] = nil
		count++
	}

	atomic.Submit()

	srv.watch.Emit(events)
	return next
}

func (srv *Service) emitSchedule(now uint64) uint64 {
	atomic := srv.db.Atomic()
	defer atomic.Cancel()

	var scheduleKey helper.ScheduleKey
	var holder api.QueueValueHolder
	next := uint64(0)
	buf := make([]byte, 1025)
	events := make(map[string][]api.QueueEntry, 0)
	for key := range srv.schedule.All {
		scheduleKey.Decode(key)
		if scheduleKey.Schedule > now {
			if next > 0 {
				next = min(next, scheduleKey.Schedule)
			} else {
				next = scheduleKey.Schedule
			}
			break
		}
		srv.schedule.Delete(key)
		raw := srv.queue.Get(buf, helper.QueueKey{
			Key:      scheduleKey.Key,
			Schedule: scheduleKey.Schedule,
			Sequence: scheduleKey.Sequence,
		}.Encode())
		if raw != nil {
			holder.Decode(raw)
			events[string(scheduleKey.Key)] = append(events[string(scheduleKey.Key)], api.QueueEntry{
				Key:      scheduleKey.Key,
				Value:    holder.Value,
				Encoding: holder.Encoding,
				Schedule: scheduleKey.Schedule,
				Sequence: scheduleKey.Sequence,
			})
		}
	}

	atomic.Submit()

	srv.listen.Emit(events)
	return next
}

func (srv *Service) Run(g *group.Group) {
	srv.cleanExpires(uint64(time.Now().UnixMilli()))
	srv.emitSchedule(uint64(time.Now().UnixMilli()))
	srv.watch.Run(g)
	srv.listen.Run(g)
	srv.expiresAlarm.Run(g, srv.cleanExpires)
	srv.scheduleAlarm.Run(g, srv.emitSchedule)
}

func (srv *Service) Close() {
	srv.db.Close()
}
