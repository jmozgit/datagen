package taskbuilder

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/samber/mo"
)

func buildTableTask(
	ctx context.Context,
	registry generatorRegistry,
	schemaProvider model.SchemaProvider,
	target config.Target,
) (model.TaskGenerators, error) {
	schemaAwareID, err := schemaProvider.TargetIdentifier(target)
	if err != nil {
		return model.TaskGenerators{}, fmt.Errorf("%w: build table task", err)
	}

	schema, err := schemaProvider.DataSource(ctx, schemaAwareID)
	if err != nil {
		return model.TaskGenerators{}, fmt.Errorf("%w: build table task", err)
	}

	userSettingsByID := make(map[model.Identifier]config.Generator)
	for _, settings := range target.Table.Generators {
		id, err := schemaProvider.GeneratorIdentifier(settings)
		if err != nil {
			return model.TaskGenerators{}, fmt.Errorf("%w: build table task", err)
		}

		userSettingsByID[id] = settings
	}

	gens := make([]model.Generator, 0, len(schema.DataTypes))
	for _, targetType := range schema.DataTypes {
		userSettings := mo.None[config.Generator]()
		if set, ok := userSettingsByID[targetType.SourceName]; ok {
			userSettings = mo.Some(set)
		}

		req := contract.AcceptRequest{
			Dataset:      schema,
			UserSettings: userSettings,
			BaseType:     mo.Some(targetType),
		}

		gen, err := registry.GetGenerator(ctx, req)
		if err != nil {
			return model.TaskGenerators{}, fmt.Errorf("%w: build table task", err)
		}

		gens = append(gens, gen)
	}

	return model.TaskGenerators{
		Task: model.Task{
			Schema: schema,
			Limit: model.TaskProgress{
				Rows:  target.Table.LimitRows,
				Bytes: uint64(target.Table.LimitBytes),
			},
		},
		Generators: gens,
	}, nil
}
