package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"nanokv-go/internal/group"
	"nanokv-go/internal/mkv"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/kouhin/envflag"
)

func main() {
	addrPtr := flag.String("address", ":2256", "address to listen on")
	dbPtr := flag.String("db", "data.db", "path to database file")
	certPtr := flag.String("cert", "", "path to tls cert file")
	keyPtr := flag.String("key", "", "path to tls cert file")
	syncIntervalPtr := flag.Duration("sync", time.Second, "sync interval")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "nanokv - simple key-value store with expiration and queue\n")
		flag.PrintDefaults()
	}
	envflag.Parse()
	addr := *addrPtr
	dbPath := *dbPtr

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	g := group.CreateGroup(ctx)

	db, err := mkv.OpenDB(dbPath, mkv.DBOptions{SyncInterval: *syncIntervalPtr})
	if err != nil {
		panic(err)
	}

	srv := configServer(g, db)
	web := &http.Server{Addr: addr}
	defer srv.Close()

	g.Go(func(context.Context) {
		var err error
		if len(*certPtr) > 0 && len(*keyPtr) > 0 {
			err = web.ListenAndServeTLS(*certPtr, *keyPtr)
		} else {
			err = web.ListenAndServe()
		}
		if err == http.ErrServerClosed {
			return
		}
		panic(err)
	})

	done := g.WaitChan()

	if len(*certPtr) > 0 && len(*keyPtr) > 0 {
		log.Println("server started at", web.Addr, "with TLS")
	} else {
		log.Println("server started at", web.Addr)
	}
	select {
	case <-ctx.Done():
		log.Println("interrupted, exiting")
		defer func() {
			if err := <-done; err != nil {
				log.Fatalln("unexpected error after shutdown:", err)
			}
		}()
	case err := <-done:
		log.Println("unexpected error during startup:", err)
	}
	stop()
	g.Cancel(nil)
	web.Shutdown(g.Context())
}
