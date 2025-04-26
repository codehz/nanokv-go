package service

import (
	"bytes"
	"context"
	"nanokv-go/api"
	"nanokv-go/internal/helper"
	"time"
)

type ListenHandler struct {
	serivce    *Service
	sender     PacketSend
	token      uint64
	interested map[string][]byte
	channel    chan map[string][]api.QueueEntry
}

func (serivce *Service) CreateListenHandler(sender PacketSend) WebSocketHandler {
	return &ListenHandler{
		serivce:    serivce,
		sender:     sender,
		interested: make(map[string][]byte),
		channel:    make(chan map[string][]api.QueueEntry),
	}
}

func (handler *ListenHandler) Attach(ctx context.Context) {
	cleanup := func() {
		if handler.token != 0 {
			handler.serivce.listen.RemoveListener(handler.token)
		}
	}
	for {
		select {
		case <-ctx.Done():
			cleanup()
			return
		case events := <-handler.channel:
			handler.sender.Send(api.BuildListenOutput(func(yield func(api.QueueEntry) bool) {
				for _, arr := range events {
					for _, entry := range arr {
						if !yield(entry) {
							return
						}
					}
				}
			}))
		}
	}
}

func (handler *ListenHandler) ProcessMessage(ctx context.Context, srv *Service, data []byte) error {
	parsed := api.ParseListen(data)
	added := make(map[string]struct{})
	removed := make(map[string]struct{})
	for _, key := range parsed.Added {
		added[string(key)] = struct{}{}
		handler.interested[string(key)] = key
	}
	for _, key := range parsed.Removed {
		removed[string(key)] = struct{}{}
		delete(handler.interested, string(key))
	}
	if handler.token == 0 {
		handler.token = handler.serivce.listen.NewListener(handler.channel, ctx.Done())
	}
	handler.serivce.listen.AddSubscription(handler.token, added)
	handler.serivce.listen.RemoveSubscription(handler.token, removed)

	view := handler.serivce.db.View()
	defer view.Close()
	var queueKey helper.QueueKey
	var queueValue api.QueueValueHolder
	now := uint64(time.Now().UnixMilli())
	out := api.BuildListenOutput(func(yield func(api.QueueEntry) bool) {
		for _, prefix := range handler.interested {
			for key, value := range handler.serivce.queue.IterFrom(helper.QueueKey{Key: prefix}.Encode(), false) {
				queueKey.Decode(key)
				if !bytes.Equal(queueKey.Key, prefix) || queueKey.Schedule > now {
					break
				}
				queueValue.Decode(value)
				if !yield(api.QueueEntry{
					Key:      queueKey.Key,
					Value:    queueValue.Value,
					Encoding: queueValue.Encoding,
					Schedule: queueKey.Schedule,
					Sequence: queueKey.Sequence,
				}) {
					return
				}
			}
		}
	})
	handler.sender.Send(out)
	return nil
}
