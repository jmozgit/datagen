package taskbuilder

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/schema/postgres"

	"github.com/samber/mo"
)

var (
	ErrUnknownConnectionType = errors.New("connection type is unknown")
	ErrUnsupportedTargetType = errors.New("unsupported target type")
)

type schemaProvider interface {
	TargetIdentifier(target config.Target) (model.Identifier, error)
	GeneratorIdentifier(gen config.Generator) (model.Identifier, error)
	DataSource(ctx context.Context, id model.Identifier) (model.DatasetSchema, error)
}

type generatorRegistry interface {
	GetGenerator(
		ctx context.Context,
		userValues config.Generator,
		optBaseType mo.Option[model.TargetType],
	) (model.Generator, error)
}

func makeSchemaProvider(cfg config.Config) (schemaProvider, error) {
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

	tasks := make([]model.TaskGenerators, 0, len(cfg.Targets))
	for _, task := range cfg.Targets {
		table := task.Table
		if table == nil {
			return nil, fmt.Errorf("%w: build tasks", ErrUnsupportedTargetType)
		}

		tableTask, err := buildTableTask(ctx, registry, schemaProvider, task)
		if err != nil {
			return nil, fmt.Errorf("%w: build tasks", err)
		}

		tasks = append(tasks, tableTask)
	}

	return tasks, nil
}
