package boolean

import (
	"context"
	"math/rand/v2"
)

type generator struct {
	truePercent int
}

func NewBoolean(truePercent int) generator {
	return generator{truePercent: truePercent}
}

func (g generator) Gen(_ context.Context) (any, error) {
	return rand.IntN(100) <= g.truePercent, nil
}
