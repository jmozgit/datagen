package taskbuilder

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator/reference"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/samber/mo"
)

var ErrUnresolvedReference = errors.New("reference is unresolved")

type referenceRequest struct {
	info           model.ReferenceInfo
	taskIdx        int
	generatorIndex int
}

func buildTableTask(
	ctx context.Context,
	registry generatorRegistry,
	schemaProvider model.SchemaProvider,
	target config.Target,
) (
	model.TaskGenerators,
	[]referenceRequest,
	error,
) {
	schemaAwareID, err := schemaProvider.TargetIdentifier(target)
	if err != nil {
		return model.TaskGenerators{}, nil, fmt.Errorf("%w: build table task", err)
	}

	schema, err := schemaProvider.DataSource(ctx, schemaAwareID)
	if err != nil {
		return model.TaskGenerators{}, nil, fmt.Errorf("%w: build table task", err)
	}

	userSettingsByID := make(map[model.Identifier]config.Generator)
	for _, settings := range target.Table.Generators {
		id, err := schemaProvider.GeneratorIdentifier(settings)
		if err != nil {
			return model.TaskGenerators{}, nil, fmt.Errorf("%w: build table task", err)
		}

		userSettingsByID[id] = settings
	}

	refRequests := make([]referenceRequest, 0)
	gens := make([]model.Generator, 0, len(schema.DataTypes))
	for idx, targetType := range schema.DataTypes {
		userSettings := mo.None[config.Generator]()
		if set, ok := userSettingsByID[targetType.SourceName]; ok {
			userSettings = mo.Some(set)
		}

		if targetType.Type == model.Reference {
			refRequests = append(refRequests, referenceRequest{
				info:           targetType.ReferenceInfo,
				taskIdx:        -1,
				generatorIndex: idx,
			})

			continue
		}

		req := contract.AcceptRequest{
			Dataset:      schema,
			UserSettings: userSettings,
			BaseType:     mo.Some(targetType),
		}

		gen, err := registry.GetGenerator(ctx, req)
		if err != nil {
			return model.TaskGenerators{}, nil, fmt.Errorf("%w: build table task", err)
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
	}, refRequests, nil
}

func tryTroResolve(
	ctx context.Context,
	req referenceRequest,
	generators []model.TaskGenerators,
) ([]model.TaskGenerators, error) {
	for _, gen := range generators {
		if gen.Schema.ID != req.info.RefDataschema {
			continue
		}

		for i, col := range gen.Schema.DataTypes {
			if col.SourceName != req.info.RefTargetType {
				continue
			}

			// it might be recursive ?
			pipe := reference.BuildPipe(gen.Generators[i], 100)
			gen.Generators[i] = pipe.Pub
			generators[req.taskIdx].Generators[req.generatorIndex] = pipe.Sub

			return generators, nil
		}
	}

	return nil, fmt.Errorf("%w: try to resolve", ErrUnresolvedReference)
}

func resolveReference(
	ctx context.Context,
	generators []model.TaskGenerators,
	requests []referenceRequest,
) ([]model.TaskGenerators, error) {
	for _, req := range requests {
		var err error

		generators, err = tryTroResolve(ctx, req, generators)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve reference %s.%s", err, req.info.RefDataschema, req.info.RefTargetType)
		}
	}

	return generators, nil
}
