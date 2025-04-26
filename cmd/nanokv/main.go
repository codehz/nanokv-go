package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"nanokv-go/internal/group"
	"net/http"
	"os"
	"os/signal"
)

func main() {
	addrPtr := flag.String("address", ":2256", "address to listen on")
	dbPtr := flag.String("db", "data.db", "path to database file")
	certPtr := flag.String("cert", "", "path to tls cert file")
	keyPtr := flag.String("key", "", "path to tls cert file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "nanokv - simple key-value store with expiration and queue\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	addr := *addrPtr
	dbPath := *dbPtr

	g := group.CreateGroup(context.Background())

	srv := configServer(dbPath, g)
	web := &http.Server{Addr: addr}
	defer srv.Close()

	g.Go(func(context.Context) {
		var err error
		if certPtr != nil && keyPtr != nil {
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

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	if certPtr != nil && keyPtr != nil {
		log.Println("server started at", web.Addr, "with TLS")
	} else {
		log.Println("server started at", web.Addr)
	}
	select {
	case <-sig:
		log.Println("interrupted, exiting")
		signal.Reset(os.Interrupt)
		g.Cancel(nil)
		web.Shutdown(g.Context())
		if err := <-done; err != nil {
			log.Fatalln("unexpected error after shutdown:", err)
		}
	case err := <-done:
		log.Println("unexpected error during startup:", err)
		signal.Reset(os.Interrupt)
		g.Cancel(nil)
		web.Shutdown(g.Context())
	}
}
