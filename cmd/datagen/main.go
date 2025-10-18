package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/viktorkomarov/datagen/cmd/datagen/command"
)

func main() {
	appCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cmd := command.New(appCtx)
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
