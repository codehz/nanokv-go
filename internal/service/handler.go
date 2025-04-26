package service

import "context"

type WebSocketHandler interface {
	Attach(ctx context.Context)
	ProcessMessage(ctx context.Context, srv *Service, data []byte) error
}
