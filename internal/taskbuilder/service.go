package taskbuilder

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/refresolver"
	"github.com/viktorkomarov/datagen/internal/schema/postgres"
)

var (
	ErrUnknownConnectionType = errors.New("connection type is unknown")
	ErrUnsupportedTargetType = errors.New("unsupported target type")
)

type generatorRegistry interface {
	GetGenerator(
		ctx context.Context,
		req contract.AcceptRequest,
	) (model.Generator, error)
}

func makeSchemaProvider(cfg config.Config) (model.SchemaProvider, error) {
	switch cfg.Connection.Type {
	case config.PostgresqlConnection:
		inspector, err := postgres.NewInspector(cfg.Connection.Postgresql)
		if err != nil {
			return nil, fmt.Errorf("%w: make schema provider", err)
		}

		return inspector, nil
	default:
		return nil, fmt.Errorf("%w: make schema provider", ErrUnknownConnectionType)
	}
}

func Build(
	ctx context.Context,
	cfg config.Config,
	registry generatorRegistry,
	refSvc *refresolver.Service,
) ([]model.Task, error) {
	const fnName = "taskbuilder: build"

	schemaProvider, err := makeSchemaProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	ttb := newTableTaskBuilder(cfg, schemaProvider, registry, refSvc)
	for _, task := range cfg.Targets {
		table := task.Table
		if table == nil {
			return nil, fmt.Errorf("%w: %s", ErrUnsupportedTargetType, fnName)
		}

		if err := ttb.addTableTask(ctx, table); err != nil {
			return nil, fmt.Errorf("%w: %s", err, fnName)
		}
	}

	return ttb.sortTasks()
}
