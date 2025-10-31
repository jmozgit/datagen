package array

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/model"
)

type Generator struct {
	rows int
	cols int
	gen  model.Generator
}

func NewGenerator(
	rows int,
	cols int,
	gen model.Generator,
) model.Generator {
	return &Generator{
		rows: rows,
		cols: cols,
		gen:  gen,
	}
}

func (g *Generator) Gen(ctx context.Context) (any, error) {
	const fnName = "array: gen"

	vals := make([][]any, g.rows)
	for i := range vals {
		vals[i] = make([]any, g.cols)
	}

	var err error
	for i := range vals {
		for j := range vals[i] {
			vals[i][j], err = g.Gen(ctx)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", err, fnName)
			}
		}
	}

	return vals, nil
}

func (g *Generator) Close() {
	g.gen.Close()
}
