package null

import (
	"context"
	"math/rand/v2"

	"github.com/jmozgit/datagen/internal/model"
)

type generator struct {
	nullFraction  int
	baseGenerator model.Generator
}

func NewGenerator(
	nullFraction int,
	baseGenerator model.Generator,
) model.Generator {
	return &generator{
		nullFraction:  nullFraction,
		baseGenerator: baseGenerator,
	}
}

func (g *generator) Gen(ctx context.Context) (any, error) {
	if rand.IntN(100) <= g.nullFraction {
		return nil, nil
	}

	return g.baseGenerator.Gen(ctx)
}

func (g *generator) Close() {
	g.baseGenerator.Close()
}
