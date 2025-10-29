package probability

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/jmozgit/datagen/internal/model"
)

var ErrInvalidDistValues = errors.New("invalid dist or values")

type List[T any] struct {
	values    []T
	prefixSum []int
	totalSum  int
}

func NewList[T any](dist []int, values []T) (model.Generator, error) {
	const fnName = "probability: new list"

	if len(dist) == 0 {
		return nil, fmt.Errorf("%w: %s len(dist) == 0", ErrInvalidDistValues, fnName)
	}

	if len(dist) != len(values) {
		return nil, fmt.Errorf("%w: %s len(dist) != len(values)", ErrInvalidDistValues, fnName)
	}

	prefixSum := make([]int, len(dist))
	totalSum := 0
	for i, d := range dist {
		totalSum += d
		prefixSum[i] = totalSum
	}

	return &List[T]{
		values:    values,
		prefixSum: prefixSum,
		totalSum:  totalSum,
	}, nil
}

func (l *List[T]) Gen(_ context.Context) (any, error) {
	sector := rand.IntN(l.totalSum + 1)
	for i := range l.prefixSum {
		if sector <= l.prefixSum[i] {
			return l.values[i], nil
		}
	}
	return l.values[0], nil
}

func (l *List[T]) Close() {}
