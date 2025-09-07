package main

import (
	"log"

	"github.com/viktorkomarov/datagen/cmd/datagen/command"
)

func main() {
	cmd := command.New()

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
