package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viktorkomarov/datagen/internal/acceptor/commontype"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/acceptor/user"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/closer"
	"github.com/viktorkomarov/datagen/internal/pkg/db/adapter/pgx"
	"github.com/viktorkomarov/datagen/internal/refresolver"
)

type Registry struct {
	refRegistry *refresolver.Service
	providers   []contract.GeneratorProvider
}

func PrepareRegistry(
	ctx context.Context,
	cfg config.Config,
	refRegistry *refresolver.Service,
	closerReg *closer.Registry,
) (*Registry, error) {
	generators := append(
		user.DefaultProviderGenerators(),
		commontype.DefaultProviderGenerators()...,
	)

	switch cfg.Connection.Type {
	case config.PostgresqlConnection:
		pool, err := pgxpool.New(ctx, cfg.Connection.ConnString())
		if err != nil {
			return nil, fmt.Errorf("%w: prepare registry", err)
		}
		closerReg.Add(closer.Fn(pool.Close))

		generators = append(
			generators,
			postgresql.DefaultProviderGenerators(
				pgx.NewAdapterPool(pool),
				refRegistry,
			)...)
	default:
	}

	return &Registry{
		refRegistry: refRegistry,
		providers:   generators,
	}, nil
}

func (r *Registry) GetGenerator(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.Generator, error) {
	const fnName = "get generator"

	matched := make(map[model.AcceptanceReason][]model.Generator)
	for _, provider := range r.providers {
		decision, err := provider.Accept(ctx, req)
		if err == nil {
			matched[decision.AcceptedBy] = append(matched[decision.AcceptedBy], decision.Generator)

			continue
		}

		if errors.Is(err, contract.ErrGeneratorDeclined) {
			continue
		}

		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	priority := []model.AcceptanceReason{
		model.AcceptanceUserSettings,
		model.AcceptanceReasonDriverAwareness,
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

	return nil, fmt.Errorf("%w: %s", contract.ErrNoAvailableGenerators, fnName)
}
