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

	var err error
	if g.rows == 1 {
		vals := make([]any, g.cols)
		for i := range vals {
			vals[i], err = g.gen.Gen(ctx)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", err, fnName)
			}
		}

		return vals, nil
	}

	vals := make([][]any, g.rows)
	for i := range g.rows {
		row := make([]any, g.cols)
		for j := range g.cols {
			row[j], err = g.gen.Gen(ctx)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", err, fnName)
			}
		}
		vals[i] = row
	}

	return vals, nil
}

func (g *Generator) Close() {
	g.gen.Close()
}
