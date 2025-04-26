package emitter

import (
	"context"
	"nanokv-go/internal/group"
)

type listener[T any] struct {
	subscriptions map[string]struct{}
	output        chan<- map[string]T
	done          <-chan struct{}
}

type Emitter[T any] struct {
	lastid        uint64
	genid         chan uint64
	listeners     map[uint64]listener[T]
	subscriptions map[string]map[uint64]struct{}
	mailbox       chan func()
}

func NewEmitter[T any](buffersize int) *Emitter[T] {
	return &Emitter[T]{
		lastid:        0,
		genid:         make(chan uint64),
		listeners:     make(map[uint64]listener[T]),
		subscriptions: make(map[string]map[uint64]struct{}),
		mailbox:       make(chan func(), buffersize),
	}
}

func (emitter *Emitter[T]) Run(g *group.Group) {
	g.Go(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				close(emitter.genid)
			case emitter.genid <- emitter.lastid:
				emitter.lastid++
			}
		}
	})
	g.Go(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case fn := <-emitter.mailbox:
				fn()
			}
		}
	})
}

func (emitter *Emitter[T]) NewListener(output chan<- map[string]T, done <-chan struct{}) uint64 {
	id := <-emitter.genid
	emitter.mailbox <- func() {
		emitter.listeners[id] = listener[T]{subscriptions: make(map[string]struct{}), output: output, done: done}
	}
	return id
}

func (emitter *Emitter[T]) RemoveListener(id uint64) {
	emitter.mailbox <- func() {
		if listener, ok := emitter.listeners[id]; ok {
			for key := range listener.subscriptions {
				delete(emitter.subscriptions[key], id)
			}
			delete(emitter.listeners, id)
		}
	}
}

func (emitter *Emitter[T]) AddSubscription(id uint64, keys map[string]struct{}) {
	emitter.mailbox <- func() {
		if listener, ok := emitter.listeners[id]; ok {
			for key := range keys {
				listener.subscriptions[key] = struct{}{}
				if _, ok := emitter.subscriptions[key]; !ok {
					emitter.subscriptions[key] = make(map[uint64]struct{})
				}
				emitter.subscriptions[key][id] = struct{}{}
			}
		}
	}
}

func (emitter *Emitter[T]) RemoveSubscription(id uint64, keys map[string]struct{}) {
	emitter.mailbox <- func() {
		if listener, ok := emitter.listeners[id]; ok {
			for key := range keys {
				delete(listener.subscriptions, key)
				delete(emitter.subscriptions[key], id)
				if len(emitter.subscriptions[key]) == 0 {
					delete(emitter.subscriptions, key)
				}
			}
		}
	}
}

func (emitter *Emitter[T]) Emit(events map[string]T) {
	if len(events) == 0 {
		return
	}
	emitter.mailbox <- func() {
		pending := make(map[uint64]map[string]T)
		for key, event := range events {
			if listeners, ok := emitter.subscriptions[key]; ok {
				for id := range listeners {
					if _, ok := pending[id]; !ok {
						pending[id] = make(map[string]T)
					}
					pending[id][key] = event
				}
			}
		}
		for id, events := range pending {
			if listener, ok := emitter.listeners[id]; ok {
				select {
				case <-listener.done:
					for key := range listener.subscriptions {
						delete(emitter.subscriptions[key], id)
					}
					delete(emitter.listeners, id)
					continue
				case listener.output <- events:
				}
			}
		}
	}
}
