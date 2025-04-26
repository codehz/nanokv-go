package helper

import (
	"context"
	"nanokv-go/internal/group"
	"time"
)

type Alarm struct {
	timer *time.Timer
	last  uint64
	next  chan uint64
}

func NewAlarm() *Alarm {
	return &Alarm{
		next: make(chan uint64),
	}
}

func (alarm *Alarm) Run(g *group.Group, fn func(now uint64) uint64) {
	g.Go(func(ctx context.Context) {
		for {
			var next uint64
			if alarm.timer != nil {
				select {
				case <-ctx.Done():
					return
				case <-alarm.timer.C:
					now := uint64(time.Now().UnixMilli())
					alarm.last = 0
					next = fn(now)
				case next = <-alarm.next:
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case next = <-alarm.next:
				}
			}
			if next == 0 {
				continue
			}
			for {
				target := alarm.schedule(next)
				if target == 0 {
					break
				}
				now := uint64(time.Now().UnixMilli())
				next = fn(now)
			}
		}
	})
}

func (alarm *Alarm) schedule(target uint64) uint64 {
	if alarm.last != 0 {
		if alarm.last < target {
			return 0
		}
	}
	now := uint64(time.Now().UnixMilli())
	diff := time.Duration(target) - time.Duration(now)
	if diff <= 0 {
		return target
	}
	alarm.last = target
	if alarm.timer != nil {
		alarm.timer.Reset(time.Millisecond * diff)
	} else {
		alarm.timer = time.NewTimer(time.Millisecond * diff)
	}
	return 0
}

func (alarm *Alarm) Schedule(target uint64) {
	if target == 0 {
		return
	}
	alarm.next <- target
}
