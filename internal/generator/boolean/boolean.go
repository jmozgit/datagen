package boolean

import (
	"context"
	"math/rand/v2"

	"github.com/viktorkomarov/datagen/internal/model"
)

type generator struct {
	truePercent int
}

func NewBoolean(truePercent int) model.Generator {
	return generator{truePercent: truePercent}
}

func (g generator) Gen(_ context.Context) (any, error) {
	return rand.IntN(100) <= g.truePercent, nil
}

func (g generator) Close() {}
