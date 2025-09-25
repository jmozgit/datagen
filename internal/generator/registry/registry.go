package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/closer"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/mo"
)

type Registry struct {
	providers []model.GeneratorProvider
}

func PrepareRegistry(
	ctx context.Context,
	cfg config.Config,
	closerReg *closer.Registry,
) (*Registry, error) {
	generators := defaultGeneratorProviders()

	switch cfg.Connection.Type {
	case config.PostgresqlConnection:
		pool, err := pgxpool.New(ctx, cfg.Connection.ConnString())
		if err != nil {
			return nil, fmt.Errorf("%w: prepare registry", err)
		}
		closerReg.Add(closer.Fn(pool.Close))

		pgSpecificGenerators, err := postgresql.DefaultProviderGenerators(pool)
		if err != nil {
			return nil, fmt.Errorf("%w: prepare registry", err)
		}

		generators = append(generators, pgSpecificGenerators...)
	default:
	}

	return &Registry{
		providers: generators,
	}, nil
}

func (r *Registry) GetGenerator(
	ctx context.Context,
	dataset model.DatasetSchema,
	userValues mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (model.Generator, error) {
	const fnName = "get generator"

	matched := make(map[model.AcceptanceReason][]model.Generator)
	for _, provider := range r.providers {
		decision, err := provider.Accept(ctx, dataset, userValues, optBaseType)
		if err == nil {
			matched[decision.AcceptedBy] = append(matched[decision.AcceptedBy], decision.Generator)

			continue
		}

		if errors.Is(err, generator.ErrGeneratorDeclined) {
			continue
		}

		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	priority := []model.AcceptanceReason{
		model.AcceptanceUserSettings,
		model.AcceptanceReasonColumnType,
		model.AcceptanceReasonDomain,
		model.AcceptanceReasonColumnNameSuggestion,
	}

	for _, reason := range priority {
		if gens, ok := matched[reason]; ok {
			// what to do if len(gens) > 0 ?
			return gens[0], nil
		}
	}

	return nil, fmt.Errorf("%w: %s", generator.ErrNoAvailableGenerators, fnName)
}
