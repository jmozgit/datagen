package fn

import (
	"context"

	"github.com/jmozgit/datagen/internal/model"
)

type generator struct {
	fn func(ctx context.Context) (any, error)
}

func NewGenerator(
	fn func(ctx context.Context) (any, error),
) model.Generator {
	return generator{fn: fn}
}

func (g generator) Gen(ctx context.Context) (any, error) {
	return g.fn(ctx)
}

func (g generator) Close() {}
