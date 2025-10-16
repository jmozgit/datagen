package taskbuilder

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/refresolver"

	"github.com/samber/lo"
	"github.com/samber/mo"
)

var ErrCycledRefences = errors.New("cycled refernces are not allowed")

type tableTaskBuilder struct {
	targets        []config.Table
	tasks          []model.TaskGenerators
	refresolver    *refresolver.Service
	schemaProvider model.SchemaProvider
}

func newTableTaskBuilder(
	schemaProvider model.SchemaProvider,
	refresolver *refresolver.Service,
) tableTaskBuilder {
	return tableTaskBuilder{
		targets:        make([]config.Table, 0),
		tasks:          make([]model.TaskGenerators, 0),
		refresolver:    refresolver,
		schemaProvider: schemaProvider,
	}
}

func (t *tableTaskBuilder) addTableTask(ctx context.Context, target *config.Table) error {
	const fnName = "add table task"

	schemaAwareID, err := t.schemaProvider.TableIdentifier(ctx, target)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	schema, err := t.schemaProvider.Table(ctx, schemaAwareID)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	task := model.TaskGenerators{
		Task: model.Task{
			DatasetSchema: schema,
			Limit: model.TaskProgress{
				Rows:  target.LimitRows,
				Bytes: uint64(target.LimitBytes),
			},
		},
		Generators: make([]model.Generator, len(schema.Columns)),
	}
	t.targets = append(t.targets, *target)
	t.tasks = append(t.tasks, task)

	return nil
}

func (t *tableTaskBuilder) setGenerators(ctx context.Context, registry generatorRegistry) error {
	const fnName = "set generators"

	for idx, target := range t.targets {
		task := t.tasks[idx]
		userSettingsByID := make(map[model.Identifier]config.Generator)
		for _, settings := range target.Generators {
			id, err := t.schemaProvider.ColumnIdentifier(ctx, task.DatasetSchema.TableName, settings.Column)
			if err != nil {
				return fmt.Errorf("%w: %s", err, fnName)
			}

			userSettingsByID[id] = settings
		}

		for genIdx, targetType := range task.DatasetSchema.Columns {
			userSettings := mo.None[config.Generator]()
			if set, ok := userSettingsByID[targetType.SourceName]; ok {
				delete(userSettingsByID, targetType.SourceName)
				userSettings = mo.Some(set)
			}

			req := contract.AcceptRequest{
				Dataset:      task.DatasetSchema,
				UserSettings: userSettings,
				BaseType:     mo.Some(targetType),
			}

			gen, err := registry.GetGenerator(ctx, req)
			if err != nil {
				return fmt.Errorf(
					"%w: %s.%s %s",
					err,
					task.DatasetSchema.TableName.Quoted(),
					targetType.SourceName.AsArgument(),
					fnName,
				)
			}

			t.tasks[idx].Generators[genIdx] = gen
		}

		if len(userSettingsByID) > 0 {
			return fmt.Errorf("unknown user config %s", fnName)
		}
	}

	return nil
}

func (t *tableTaskBuilder) sortTasks() ([]model.TaskGenerators, error) {
	byID := lo.SliceToMap(t.tasks, func(t model.TaskGenerators) (model.TableName, model.TaskGenerators) {
		return t.DatasetSchema.TableName, t
	})

	ids := slices.Collect(maps.Keys(byID))

	sortedIDs, err := topSort(ids, t.refresolver.DepsOn())
	if err != nil {
		return nil, fmt.Errorf("%w: sort tasks", err)
	}

	return lo.FilterMap(sortedIDs, func(t model.TableName, _ int) (model.TaskGenerators, bool) {
		gen, ok := byID[t]

		return gen, ok
	}), nil
}

func topSort(
	ids []model.TableName,
	deps map[model.TableName][]model.TableName,
) ([]model.TableName, error) {
	const fnName = "top sort"

	out := make([]model.TableName, 0, len(ids))
	visited := make(map[model.TableName]bool)
	inProgress := make(map[model.TableName]bool)
	var visit func(model.TableName) error

	visit = func(id model.TableName) error {
		const fnName = "inner visit"

		if visited[id] {
			return nil
		}

		if inProgress[id] {
			return fmt.Errorf("%w: %s %s", ErrCycledRefences, id.Quoted(), fnName)
		}

		inProgress[id] = true
		for _, d := range deps[id] {
			if err := visit(d); err != nil {
				return fmt.Errorf("%w: %s", err, fnName)
			}
		}

		visited[id] = true
		out = append(out, id)

		return nil
	}

	for _, id := range ids {
		if err := visit(id); err != nil {
			return nil, fmt.Errorf("%w: %s", err, fnName)
		}
	}

	return out, nil
}
