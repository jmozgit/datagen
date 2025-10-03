package bytea

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"

	"github.com/viktorkomarov/datagen/internal/model"
)

type aroundByteaGenerator struct {
	size    int
	maxDiff int
}

func NewAroundByteaGenerator(size, maxDiff int) model.Generator {
	return aroundByteaGenerator{size: size}
}

func (a aroundByteaGenerator) Gen(_ context.Context) (any, error) {
	size := a.size - a.maxDiff + rand.IntN(2*a.maxDiff)

	buff := make([]byte, size)

	_, err := crand.Read(buff)
	if err != nil {
		return nil, fmt.Errorf("%w: bytea gen", err)
	}

	return buff, nil
}
