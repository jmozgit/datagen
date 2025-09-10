package generator

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
)

var (
	ErrGeneratorAlreadyRegistered = errors.New("generator is already registered")
	ErrUnknowDomainContextKey     = errors.New("unknown domain context key")
)

type Registry struct {
	generators map[model.DomainContextKey]model.Generator
}

type TypedGenerator interface {
	model.Generator
	Type() model.DomainContextKey
}

func NewRegistry(gens ...TypedGenerator) (*Registry, error) {
	r := &Registry{
		generators: make(map[model.DomainContextKey]model.Generator),
	}

	for _, gen := range gens {
		key := gen.Type()

		if _, ok := r.generators[key]; ok {
			return nil, fmt.Errorf("%w: new registry %s", ErrGeneratorAlreadyRegistered, key)
		}
		r.generators[key] = gen
	}

	return r, nil
}

func (r *Registry) PickGenerator(ctx context.Context, key model.DomainContextKey) (model.Generator, error) {
	gen, ok := r.generators[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknowDomainContextKey, key)
	}

	return gen, nil
}
