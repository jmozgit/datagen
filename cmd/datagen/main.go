package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"syscall"

	"github.com/viktorkomarov/datagen/cmd/datagen/command"
)

func main() {
	appCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	cmd := command.New(appCtx)
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
