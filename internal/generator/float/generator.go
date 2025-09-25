package float

import (
	"context"
	"math/rand/v2"

	"github.com/viktorkomarov/datagen/internal/model"
)

type float32Gen struct {
}

func newFloat32Gen() model.Generator {
	return float32Gen{}
}

func (f float32Gen) Gen(_ context.Context) (any, error) {
	minF := float32(-1000000.0)
	maxF := float32(1000000.0)

	return minF + rand.Float32()*(maxF-minF), nil
}

func newFloat64Gen() model.Generator {
	return float64Gen{}
}

type float64Gen struct {
}

func (f float64Gen) Gen(_ context.Context) (any, error) {
	return rand.NormFloat64(), nil
}
