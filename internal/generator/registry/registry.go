package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/closer"

	"github.com/samber/mo"
)

type Registry struct {
	providers []model.GeneratorProvider
}

func PrepareRegistry(
	ctx context.Context,
	cfg *config.Config,
	closerReg closer.CloserRegistry,
) (*Registry, error) {
	generators := defaultGeneratorProviders()

	switch cfg.Connection.Type {
	case config.PostgresqlConnection:
		pool, err := pgxpool.New(ctx, cfg.Connection.ConnString())
		if err != nil {
			return nil, fmt.Errorf("%w: prepare registry", err)
		}
		closerReg.Add(closer.CloserFn(pool.Close))

		pgSpecificGenerators, err := postgresql.DefaultProviderGenerators(pool)
		if err != nil {
			return nil, fmt.Errorf("%w: prepare registry", err)
		}

		generators = append(generators, pgSpecificGenerators...)
	}

	return &Registry{
		providers: generators,
	}, nil
}

func (r *Registry) GetGenerator(
	ctx context.Context,
	userValues mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (model.Generator, error) {
	for _, provider := range r.providers {
		decision, err := provider.Accept(ctx, userValues, optBaseType)
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
