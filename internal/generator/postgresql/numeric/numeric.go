package numeric

import (
	"context"
	"math"
	"math/rand/v2"

	"github.com/shopspring/decimal"
	"github.com/viktorkomarov/datagen/internal/model"
)

type pgNumericGenerator struct {
	scale     int
	precision int
}

func NewPostgresqlNumericGenerator(scale int, precision int) model.Generator {
	return pgNumericGenerator{scale: scale, precision: precision}
}

func randSign() int {
	if rand.Int()%2 == 0 { //nolint:gosec // ok
		return 1
	}

	return -1
}

func (p pgNumericGenerator) Gen(_ context.Context) (any, error) {
	if p.precision == 0 {
		return math.Float64frombits(rand.Uint64()), nil //nolint:gosec // ok
	}

	if p.scale > 0 {
		diff := p.precision - p.scale
		switch {
		case diff == 0:
			minV := int(math.Pow10(p.precision - 1))
			maxV := int(math.Pow10(p.precision))
			sdigits := rand.IntN(maxV-minV) + minV //nolint:gosec // ok
			sdigits = randSign() * sdigits

			return decimal.New(int64(sdigits), -int32(p.precision)), nil
		case diff > 0:
			minV := int(math.Pow10(p.precision - 1))
			maxV := int(math.Pow10(p.precision))
			sdigits := rand.IntN(maxV-minV) + minV //nolint:gosec // ok
			sdigits = randSign() * sdigits
			freqPart := p.scale + rand.IntN(p.precision) //nolint:gosec // ok

			return decimal.New(int64(sdigits), -int32(freqPart)), nil
		case diff < 0:
			minV := int(math.Pow10(p.precision - 1))
			maxV := int(math.Pow10(p.precision))
			sdigits := rand.IntN(maxV-minV) + minV //nolint:gosec // ok
			sdigits = randSign() * sdigits

			return decimal.New(int64(sdigits), -int32(p.scale)), nil
		}
	}

	absS := int(math.Abs(float64(p.scale)))
	maxDigits := int(math.Pow10(p.precision))
	step := int(math.Pow10(absS))
	val := randSign() * rand.IntN(maxDigits) * step //nolint:gosec // ok

	return float64(val), nil
}

func (p pgNumericGenerator) Close() {}
