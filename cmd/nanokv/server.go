package main

import (
	"context"
	"errors"
	"io"
	"nanokv-go/internal/group"
	"nanokv-go/internal/helper"
	"nanokv-go/internal/mkv"
	"nanokv-go/internal/service"
	"net/http"

	"github.com/coder/websocket"
)

func configServer(path string, g *group.Group) *service.Service {
	db, err := mkv.OpenDB(path)
	if err != nil {
		panic(err)
	}
	srv := service.NewService(db)
	srv.Run(g)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("NANOKV"))
	})
	http.HandleFunc("/snapshot_read", func(w http.ResponseWriter, r *http.Request) {
		buf, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		out, err := srv.HandleSnapshotRead(buf)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(out)
	})
	http.HandleFunc("/atomic_write", func(w http.ResponseWriter, r *http.Request) {
		buf, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		out, err := srv.HandleAtomicWrite(buf)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(out)
	})
	http.Handle("/listen", WebSocketHandlerFactory{
		ctx:    g.Context(),
		srv:    srv,
		create: srv.CreateListenHandler,
	})
	http.Handle("/watch", WebSocketHandlerFactory{
		ctx:    g.Context(),
		srv:    srv,
		create: srv.CreateWatchHandler,
	})
	return srv
}

type HTTPHandlerFactory struct {
	handle func(input []byte) ([]byte, error)
}

func (factory HTTPHandlerFactory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	out, err := factory.handle(buf)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(out)
}

type WebSocketHandlerFactory struct {
	ctx    context.Context
	srv    *service.Service
	create func(sender service.PacketSend) service.WebSocketHandler
}

func (factory WebSocketHandlerFactory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	g := group.CreateGroup(factory.ctx)
	defer helper.RecoverCancelCause(g.Cancel)
	handler := factory.create(WebSocketSender{
		Conn:  conn,
		Group: g,
	})
	g.Go(handler.Attach)
	for {
		msgtype, data, err := conn.Read(g.Context())
		if err != nil {
			var closeError websocket.CloseError
			if errors.As(err, &closeError) {
				if closeError.Code == websocket.StatusNormalClosure {
					g.Cancel(nil)
					return
				}
			}
			g.Cancel(err)
			return
		}
		if msgtype != websocket.MessageBinary {
			g.Cancel(errors.New("invalid message type"))
			return
		}
		err = handler.ProcessMessage(g.Context(), factory.srv, data)
		if err != nil {
			g.Cancel(err)
			return
		}
	}
}

type WebSocketSender struct {
	*websocket.Conn
	*group.Group
}

func (sender WebSocketSender) Send(data []byte) {
	err := sender.Write(sender.Context(), websocket.MessageBinary, data)
	if err != nil {
		sender.Cancel(err)
	}
}
