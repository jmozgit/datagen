package reuse

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/jmozgit/datagen/internal/model"
)

var ErrCantTakeValue = errors.New("can't take value")

type generator struct {
	fallback      model.ColumnValueReader
	reuseFraction int
	buff          []any
	baseGenerator model.Generator
}

func NewGenerator(
	ctx context.Context,
	fallback model.ColumnValueReader,
	reuseFraction int,
	baseGenerator model.Generator,
) (model.Generator, error) {
	vals, err := fallback.ReadValues(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: reuse: new generator", err)
	}

	return &generator{
		fallback:      fallback,
		reuseFraction: reuseFraction,
		buff:          vals,
		baseGenerator: baseGenerator,
	}, nil
}

func (g *generator) Close() {
	g.baseGenerator.Close()
}

func (g *generator) Gen(ctx context.Context) (any, error) {
	if rand.IntN(100) > g.reuseFraction {
		return g.genFromGenerator(ctx)
	}

	if rand.Int()%41 == 0 {
		if err := g.resetBuf(ctx); err != nil {
			return nil, fmt.Errorf("%w: reuse gen", err)
		}
	}

	if len(g.buff) == 0 {
		if g.reuseFraction >= 100 {
			return nil, fmt.Errorf("%w: reuse gen", ErrCantTakeValue)
		}

		return g.genFromGenerator(ctx)
	}

	idx := rand.IntN(len(g.buff))
	return g.buff[idx], nil
}

func (g *generator) resetBuf(ctx context.Context) error {
	vals, err := g.fallback.ReadValues(ctx)
	if err != nil {
		return fmt.Errorf("%w: reset buf", err)
	}

	g.buff = vals

	return nil
}

func (g *generator) genFromGenerator(ctx context.Context) (any, error) {
	val, err := g.baseGenerator.Gen(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: reuse gen", err)
	}

	storeValue := rand.Int()%2 == 0
	if storeValue {
		if len(g.buff) >= 10 {
			g.buff[rand.IntN(len(g.buff))] = val
		}
		g.buff = append(g.buff, val)
	}

	return val, nil
}
