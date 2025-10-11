package taskbuilder

import (
	"context"
	"errors"
	"fmt"

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

func (t *tableTaskBuilder) addTask(ctx context.Context, target config.Target) error {
	const fnName = "add task"

	schemaAwareID, err := t.schemaProvider.TargetIdentifier(target)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	schema, err := t.schemaProvider.DataSource(ctx, schemaAwareID)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	task := model.TaskGenerators{
		Task: model.Task{
			Schema: schema,
			Limit: model.TaskProgress{
				Rows:  target.Table.LimitRows,
				Bytes: uint64(target.Table.LimitBytes),
			},
		},
		Generators: make([]model.Generator, len(schema.DataTypes)),
	}
	t.targets = append(t.targets, *target.Table)
	t.tasks = append(t.tasks, task)

	return nil
}

func (t *tableTaskBuilder) setGenerators(ctx context.Context, registry generatorRegistry) error {
	const fnName = "set generators"

	for idx, target := range t.targets {
		userSettingsByID := make(map[model.Identifier]config.Generator)
		for _, settings := range target.Generators {
			id, err := t.schemaProvider.GeneratorIdentifier(settings)
			if err != nil {
				return fmt.Errorf("%w: %s", err, fnName)
			}

			userSettingsByID[id] = settings
		}

		for genIdx, targetType := range t.tasks[idx].Schema.DataTypes {
			userSettings := mo.None[config.Generator]()
			if set, ok := userSettingsByID[targetType.SourceName]; ok {
				userSettings = mo.Some(set)
			}

			req := contract.AcceptRequest{
				Dataset:      t.tasks[idx].Schema,
				UserSettings: userSettings,
				BaseType:     mo.Some(targetType),
			}

			gen, err := registry.GetGenerator(ctx, req)
			if err != nil {
				return fmt.Errorf("%w: %s.%s %s", err, t.tasks[idx].Schema.ID, targetType.SourceName, fnName)
			}

			t.tasks[idx].Generators[genIdx] = gen
		}
	}

	return nil
}

func (t *tableTaskBuilder) sortTasks() ([]model.TaskGenerators, error) {
	byID := lo.SliceToMap(t.tasks, func(t model.TaskGenerators) (model.Identifier, model.TaskGenerators) {
		return t.Schema.ID, t
	})

	ids := lo.Map(t.tasks, func(t model.TaskGenerators, _ int) model.Identifier {
		return t.Schema.ID
	})

	sortedIDs, err := topSort(ids, t.refresolver.DepsOn())
	if err != nil {
		return nil, fmt.Errorf("%w: sort tasks", err)
	}

	return lo.FilterMap(sortedIDs, func(t model.Identifier, _ int) (model.TaskGenerators, bool) {
		gen, ok := byID[t]

		return gen, ok
	}), nil
}

func topSort(
	ids []model.Identifier,
	deps map[model.Identifier][]model.Identifier,
) ([]model.Identifier, error) {
	const fnName = "top sort"

	out := make([]model.Identifier, 0, len(ids))
	visited := make(map[model.Identifier]bool)
	inProgress := make(map[model.Identifier]bool)
	var visit func(model.Identifier) error

	visit = func(id model.Identifier) error {
		const fnName = "inner visit"

		if visited[id] {
			return nil
		}

		if inProgress[id] {
			return fmt.Errorf("%w: %s %s", ErrCycledRefences, id, fnName)
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
