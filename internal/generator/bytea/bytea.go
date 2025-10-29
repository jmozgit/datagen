package bytea

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"

	"github.com/c2h5oh/datasize"
	"github.com/jmozgit/datagen/internal/model"
)

type aroundByteaGenerator struct {
	size    datasize.ByteSize
	maxDiff datasize.ByteSize
}

func NewAroundByteaGenerator(size, maxDiff datasize.ByteSize) model.Generator {
	return aroundByteaGenerator{size: size, maxDiff: maxDiff}
}

func (a aroundByteaGenerator) Gen(_ context.Context) (any, error) {
	sign := int64(rand.Int() % 2)
	if sign == 0 {
		sign = -1
	}

	diff := int64(a.maxDiff)
	if a.maxDiff != 0 {
		diff = sign * rand.Int64N(int64(a.maxDiff))
	}

	buff := make([]byte, int(a.size)+int(diff))

	_, err := crand.Read(buff)
	if err != nil {
		return nil, fmt.Errorf("%w: bytea gen", err)
	}

	return buff, nil
}

func (a aroundByteaGenerator) Close() {}
