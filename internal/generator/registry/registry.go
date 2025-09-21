package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/generator/integer"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/samber/mo"
)

type AcceptFn func(
	ctx context.Context,
	userValues mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (generator.AcceptanceDecision, error)

type Registry struct {
	builders []AcceptFn
}

func New() *Registry {
	return &Registry{
		builders: []AcceptFn{
			integer.Accept,
		},
	}
}

func (r *Registry) GetGenerator(
	ctx context.Context,
	userValues mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (model.Generator, error) {
	for _, buildFn := range r.builders {
		decision, err := buildFn(ctx, userValues, optBaseType)
		if err == nil {
			return decision.Generator, nil
		}

		if errors.Is(err, generator.ErrGeneratorDeclined) {
			continue
		}

		return nil, fmt.Errorf("%w: get generator", err)
	}

	return nil, fmt.Errorf("%w: gen generator", generator.ErrNoAvailableGenerators)
}
