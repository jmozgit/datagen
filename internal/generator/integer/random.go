package integer

import (
	"context"
	"math"
	"math/rand/v2"
)

type randomGenerator struct {
	min int64
	max int64
}

func NewRandomInRangeGenerator(
	minV int64, maxV int64,
) *randomGenerator {
	return &randomGenerator{min: minV, max: maxV}
}

func (r *randomGenerator) Gen(_ context.Context) (any, error) {
	if r.max == r.min {
		return r.max, nil
	}

	fromMin := int64(0)
	if r.min == math.MinInt64 {
		fromMin = rand.Int64() //nolint:gosec // ok
	} else if r.min < 0 {
		fromMin = rand.Int64N(-r.min) //nolint:gosec // ok
	}

	fromMax := int64(0)
	if r.max > 0 {
		fromMax = rand.Int64N(r.max) //nolint:gosec // ok
	}

	return r.min + fromMin + fromMax, nil
}
