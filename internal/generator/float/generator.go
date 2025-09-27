package float

import (
	"context"
	"math"
	"math/rand/v2"

	"github.com/viktorkomarov/datagen/internal/model"
)

type float32Gen struct{}

func newFloat32Gen() model.Generator {
	return float32Gen{}
}

func (f float32Gen) Gen(_ context.Context) (any, error) {
	return math.Float32frombits(rand.Uint32()), nil //nolint:gosec // ok for this purpose
}

func newFloat64Gen() model.Generator {
	return float64Gen{}
}

type float64Gen struct{}

func (f float64Gen) Gen(_ context.Context) (any, error) {
	return math.Float64frombits(rand.Uint64()), nil //nolint:gosec // ok for this purpose
}
