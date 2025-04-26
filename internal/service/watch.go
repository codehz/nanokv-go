package service

import (
	"context"
	"nanokv-go/api"
)

type WatchHandler struct {
	serivce *Service
	sender  PacketSend
	token   uint64
	channel chan map[string]*api.ValueHolder
}

func (serivce *Service) CreateWatchHandler(sender PacketSend) WebSocketHandler {
	return &WatchHandler{
		serivce: serivce,
		sender:  sender,
		channel: make(chan map[string]*api.ValueHolder),
	}
}

func (handler *WatchHandler) Attach(ctx context.Context) {
	cleanup := func() {
		if handler.token != 0 {
			handler.serivce.watch.RemoveListener(handler.token)
		}
	}
	for {
		select {
		case <-ctx.Done():
			cleanup()
			return
		case events := <-handler.channel:
			handler.sender.Send(api.BuildWatchOutput(events))
		}
	}
}

func (handler *WatchHandler) ProcessMessage(ctx context.Context, srv *Service, data []byte) error {
	parsed := api.ParseWatch(data)
	keys := make(map[string]struct{})
	for _, key := range parsed.Keys {
		keys[string(key)] = struct{}{}
	}
	if parsed.Id() != 0 {
		if handler.token == 0 {
			handler.token = handler.serivce.watch.NewListener(handler.channel, ctx.Done())
		}
		handler.serivce.watch.AddSubscription(handler.token, keys)
	} else {
		if handler.token != 0 {
			handler.serivce.watch.RemoveSubscription(handler.token, keys)
		}
	}
	view := srv.db.View()
	defer view.Close()
	buf := make([]byte, 1041)
	var value api.ValueHolder
	out := parsed.BuildFirstOutput(func(key []byte) *api.ValueHolder {
		if raw := srv.main.Get(buf, key); raw != nil {
			value.Decode(raw)
			return &value
		} else {
			return nil
		}
	})
	handler.sender.Send(out)
	return nil
}
