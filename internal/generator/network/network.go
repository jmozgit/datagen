package network

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
)

var ErrUnknownNetworkType = errors.New("unknown network type")

type generator func() any

var byName = map[string]generator{}

func NewGenerator(name string) (model.Generator, error) {
	gen, ok := byName[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s new generator", ErrUnknownNetworkType, name)
	}

	return generator(gen), nil
}

func (g generator) Gen(_ context.Context) (any, error) {
	return g(), nil
}

func (g generator) Close() {}
