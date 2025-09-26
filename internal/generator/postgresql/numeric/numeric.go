package numeric

import (
	"context"
	"math"
	"math/rand/v2"

	"github.com/shopspring/decimal"
)

type pgNumericGenerator struct {
	template numericTemplate
}

func newPGNumericGenerator(tmplt numericTemplate) pgNumericGenerator {
	return pgNumericGenerator{template: tmplt}
}

func randSign() int {
	if rand.Int()%2 == 0 {
		return 1
	}

	return -1
}

func (p pgNumericGenerator) Gen(_ context.Context) (any, error) {
	if p.template.precision == 0 {
		return math.Float64frombits(rand.Uint64()), nil
	}

	if p.template.scale > 0 {
		diff := p.template.precision - p.template.scale
		switch {
		case diff == 0:
			minV := int(math.Pow10(p.template.precision - 1))
			maxV := int(math.Pow10(p.template.precision))
			sdigits := rand.IntN(maxV-minV) + minV
			sdigits = randSign() * sdigits

			return decimal.New(int64(sdigits), -int32(p.template.precision)), nil
		case diff > 0:
			minV := int(math.Pow10(p.template.precision - 1))
			maxV := int(math.Pow10(p.template.precision))
			sdigits := rand.IntN(maxV-minV) + minV
			sdigits = randSign() * sdigits
			freqPart := p.template.scale + rand.IntN(p.template.precision)

			return decimal.New(int64(sdigits), -int32(freqPart)), nil
		case diff < 0:
			minV := int(math.Pow10(p.template.precision - 1))
			maxV := int(math.Pow10(p.template.precision))
			sdigits := rand.IntN(maxV-minV) + minV
			sdigits = randSign() * sdigits

			return decimal.New(int64(sdigits), -int32(p.template.scale)), nil
		}
	}

	absS := int(math.Abs(float64(p.template.scale)))
	maxDigits := int(math.Pow10(p.template.precision))
	step := int(math.Pow10(absS))

	val := randSign() * rand.IntN(maxDigits) * step
	return float64(val), nil
}
