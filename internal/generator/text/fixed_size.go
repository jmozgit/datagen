package text

import (
	"context"
	mathrand "math/rand/v2"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/xrand"
)

type inRangeSizeGenerator struct {
	from, to int
}

func NewInRangeSizeGenerator(from, to int) model.Generator {
	return inRangeSizeGenerator{
		from: from,
		to:   to,
	}
}

func (i inRangeSizeGenerator) Gen(_ context.Context) (any, error) {
	sz := i.from + mathrand.IntN(i.to-i.from+1)
	return xrand.String(sz), nil
}

func (i inRangeSizeGenerator) Close() {}

type fixedSizeGenerator struct {
	size int
}

func NewFixedSizedStringGenerator(size int) model.Generator {
	return fixedSizeGenerator{
		size: size,
	}
}

func (a fixedSizeGenerator) Gen(_ context.Context) (any, error) {
	return xrand.String(a.size), nil
}

func (a fixedSizeGenerator) Close() {}
