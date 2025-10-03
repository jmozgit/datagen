package taskbuilder

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
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

func Build(ctx context.Context, cfg config.Config, registry generatorRegistry) ([]model.TaskGenerators, error) {
	schemaProvider, err := makeSchemaProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: build tasks", err)
	}

	refRequests := make([]referenceRequest, 0)
	tasks := make([]model.TaskGenerators, len(cfg.Targets))
	for idx, task := range cfg.Targets {
		table := task.Table
		if table == nil {
			return nil, fmt.Errorf("%w: build tasks", ErrUnsupportedTargetType)
		}

		tableTask, references, err := buildTableTask(ctx, registry, schemaProvider, task)
		if err != nil {
			return nil, fmt.Errorf("%w: build tasks", err)
		}

		tasks[idx] = tableTask

		for i := range references {
			references[i].taskIdx = idx
		}

		refRequests = append(refRequests, refRequests...)
	}

	if len(refRequests) > 0 {
		tasks, err = resolveReference(ctx, tasks, refRequests)
		if err != nil {
			return nil, fmt.Errorf("%w: build tasks", err)
		}
	}

	return tasks, nil
}
