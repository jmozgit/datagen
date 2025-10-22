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
	"github.com/viktorkomarov/datagen/internal/refresolver"
)

type Acceptors struct {
	refRegistry *refresolver.Service
	providers   []contract.GeneratorProvider
}

func PrepareAcceptors(
	ctx context.Context,
	cfg config.Config,
	refRegistry *refresolver.Service,
	closerReg *closer.Registry,
) (*Acceptors, error) {
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
				pool,
				refRegistry,
			)...)
	default:
	}

	return &Acceptors{
		refRegistry: refRegistry,
		providers:   generators,
	}, nil
}

func (r *Acceptors) GetGenerator(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.Generator, error) {
	const fnName = "get generator"

	matched := make(map[model.AcceptanceReason][]model.AcceptanceDecision)
	for _, provider := range r.providers {
		decision, err := provider.Accept(ctx, req)
		if err == nil {
			matched[decision.AcceptedBy] = append(matched[decision.AcceptedBy], decision)

			continue
		}

		if errors.Is(err, contract.ErrGeneratorDeclined) {
			continue
		}

		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	priority := []model.AcceptanceReason{
		model.AcceptanceUserSettings,
		model.AcceptanceReasonReference,
		model.AcceptanceReasonDriverAwareness,
		model.AcceptanceReasonColumnType,
		model.AcceptanceReasonDomain,
		model.AcceptanceReasonColumnNameSuggestion,
	}

	for _, reason := range priority {
		if decisions, ok := matched[reason]; ok {
			if len(decisions) > 1 {
				return nil, fmt.Errorf("%w: %s", contract.ErrTooManyGeneratorsAvailable, fnName)
			}

			d := decisions[0]
			if d.ChooseCallback != nil {
				d.ChooseCallback()
			}

			return d.Generator, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", contract.ErrNoAvailableGenerators, fnName)
}
