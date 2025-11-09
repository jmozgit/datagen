package main

import (
	"context"
	"encoding/json"

	"github.com/jmozgit/datagen/internal/pkg/xrand"
)

type Meta struct {
	Phone string `json:"phone"`
	Email string `json:"email"`
}

type SimpleJSON struct {
	Name string   `json:"name"`
	Top  []string `json:"top"`
	Age  int      `json:"age"`
	Meta Meta     `json:"meta"`
}

func Gen(ctx context.Context) (any, error) {
	val := SimpleJSON{
		Name: xrand.LowerCaseString(5),
		Top: []string{
			"football", "tv", "books",
		},
		Age: 19,
		Meta: Meta{
			Phone: "-",
			Email: "anything@gmail.com",
		},
	}

	raw, err := json.Marshal(val)

	return raw, err
}
