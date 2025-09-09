package generator

import (
	"context"

	"github.com/viktorkomarov/datagen/internal/model"
)

type Generator interface {
	Register(r *Registry) model.DomainContextKey
	Generate(ctx context.Context, req any) (any, error)
}

type Registry struct {
	generators map[model.DomainContextKey]any
}

func NewRegistry(gens ...Generator) *Registry {
	r := &Registry{
		generators: make(map[model.DomainContextKey]any),
	}

	for _, gen := range gens {
		r.generators[gen.Register(r)] = gen
	}

	return r
}
