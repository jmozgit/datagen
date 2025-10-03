package text

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
)

type fixedSizeGenerator struct {
	size int
}

func NewArbitraryFixedSizedStringGenerator(size int) model.Generator {
	return fixedSizeGenerator{
		size: size,
	}
}

func (a fixedSizeGenerator) Gen(_ context.Context) (any, error) {
	buf := make([]byte, a.size)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("%w: text fixed size string", err)
	}

	return string(buf), nil
}
