package group

import (
	"context"
	"fmt"
	"sync"
)

type Group struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelCauseFunc
}

func CreateGroup(ctx context.Context) *Group {
	ctx, cancel := context.WithCancelCause(ctx)
	return &Group{ctx: ctx, cancel: cancel}
}

func (group *Group) Context() context.Context {
	return group.ctx
}

func (group *Group) Cancel(cause error) {
	group.cancel(cause)
}

func (group *Group) Go(fn func(ctx context.Context)) {
	group.wg.Add(1)
	go func() {
		defer group.wg.Done()
		defer func() {
			if e := recover(); e != nil {
				if err, ok := e.(error); ok {
					group.cancel(err)
				} else {
					group.cancel(fmt.Errorf("recovered panic %v", e))
				}
			}
		}()
		fn(group.ctx)
	}()
}

func (group *Group) Wait() error {
	group.wg.Wait()
	err := context.Cause(group.ctx)
	if err == context.Canceled {
		return nil
	}
	return err
}

func (group *Group) WaitChan() chan error {
	done := make(chan error)
	go func() {
		done <- group.Wait()
	}()
	return done
}
