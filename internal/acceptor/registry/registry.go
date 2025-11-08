package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmozgit/datagen/internal/acceptor/commontype"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/acceptor/user"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/closer"
	"github.com/jmozgit/datagen/internal/refresolver"
	"github.com/samber/mo"
)

type Acceptors struct {
	providers []contract.GeneratorProvider

	// options based generator provider
	withNullGeneratorProvider   contract.GeneratorProvider
	reuseValueGeneratorProvider contract.GeneratorProvider
}

func PrepareAcceptors(
	ctx context.Context,
	cfg config.Config,
	refRegistry *refresolver.Service,
	closerReg *closer.Registry,
) (*Acceptors, error) {
	self := &Acceptors{}

	commonGens, err := commontype.DefaultProviderGenerators(self, self)
	if err != nil {
		return nil, fmt.Errorf("%w: prepare acceptors", err)
	}

	generators := append(
		user.DefaultProviderGenerators(self),
		commonGens...,
	)

	switch cfg.Connection.Type {
	case config.PostgresqlConnection:
		pool, err := pgxpool.New(ctx, cfg.Connection.ConnString())
		if err != nil {
			return nil, fmt.Errorf("%w: prepare registry", err)
		}
		closerReg.Add(closer.Fn(pool.Close))

		pgGens, err := postgresql.DefaultProviderGenerators(
			pool, refRegistry, self,
		)
		if err != nil {
			return nil, fmt.Errorf("%w: prepare acceptors", err)
		}

		generators = append(generators, pgGens...)
	default:
	}

	self.providers = generators

	return self, nil
}

func (r *Acceptors) SetWithNullValuesGeneratorProvider(provider contract.GeneratorProvider) error {
	if r.withNullGeneratorProvider != nil {
		return fmt.Errorf(
			"%w with null values provider by %T",
			contract.ErrOptionGeneratorAlreadySet, r.withNullGeneratorProvider,
		)
	}

	r.withNullGeneratorProvider = provider

	return nil
}

func (r *Acceptors) SetReuseValuesGeneratorProvider(provider contract.GeneratorProvider) error {
	if r.reuseValueGeneratorProvider != nil {
		return fmt.Errorf(
			"%w with reuse generator provider %T",
			contract.ErrOptionGeneratorAlreadySet, r.reuseValueGeneratorProvider,
		)
	}

	r.reuseValueGeneratorProvider = provider

	return nil
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

func (r *Acceptors) ApplyOptions(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.Generator, error) {
	const fnName = "registry: apply options"

	baseGen, ok := req.BaseGenerator.Get()
	if !ok {
		return nil, fmt.Errorf("%w: %s", contract.ErrBaseGenIsRequired, fnName)
	}

	settings, ok := req.UserSettings.Get()
	if !ok {
		return baseGen, nil
	}

	if settings.ReuseFraction != 0 {
		decision, err := r.reuseValueGeneratorProvider.Accept(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, fnName)
		}

		req.BaseGenerator = mo.Some(decision.Generator)
	}

	if settings.NullFraction != 0 {
		decision, err := r.withNullGeneratorProvider.Accept(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, fnName)
		}

		req.BaseGenerator = mo.Some(decision.Generator)
	}

	return req.BaseGenerator.MustGet(), nil
}
