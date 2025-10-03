package oneof

import (
	"context"
	"math/rand/v2"
)

type Generator[T any] struct {
	values []T
}

func NewGenerator[T any](values []T) Generator[T] {
	return Generator[T]{values: values}
}

func (g Generator[T]) Gen(_ context.Context) (any, error) {
	return g.values[rand.IntN(len(g.values))], nil
}

func (g Generator[T]) Close() {}
