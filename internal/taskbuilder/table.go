package taskbuilder

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/limit/rows"
	"github.com/viktorkomarov/datagen/internal/limit/size"
	"github.com/viktorkomarov/datagen/internal/limit/size/postgres"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
	pgxadapter "github.com/viktorkomarov/datagen/internal/pkg/db/adapter/pgx"
	"github.com/viktorkomarov/datagen/internal/progress"
	"github.com/viktorkomarov/datagen/internal/refresolver"

	"github.com/samber/lo"
	"github.com/samber/mo"
)

var (
	ErrCycledRefences              = errors.New("cycled refernces are not allowed")
	ErrMisleadingLimits            = errors.New("both limit_rows and limit_bytes cannot be set")
	ErrUnsupportedLimitSizerDriver = errors.New("unsupported limit sizer driver")
)

type tableTaskBuilder struct {
	tasks []model.Task

	collector      *progress.Controller
	lazyCommonPool db.Connect
	cfg            config.Config
	registry       generatorRegistry
	refresolver    *refresolver.Service
	schemaProvider model.SchemaProvider
}

func newTableTaskBuilder(
	cfg config.Config,
	collector *progress.Controller,
	schemaProvider model.SchemaProvider,
	registry generatorRegistry,
	refresolver *refresolver.Service,
) tableTaskBuilder {
	return tableTaskBuilder{
		cfg:            cfg,
		tasks:          make([]model.Task, 0),
		refresolver:    refresolver,
		registry:       registry,
		schemaProvider: schemaProvider,
		lazyCommonPool: nil,
		collector:      collector,
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

	if target.LimitRows != 0 && target.LimitBytes != 0 {
		return fmt.Errorf("%w: %s", ErrMisleadingLimits, fnName)
	}

	var stopper model.Stopper
	if target.LimitRows != 0 {
		stopper = rows.NewStopper(
			int64(target.LimitRows),
			schemaAwareID.String(),
			t.collector,
		)
	}
	if target.LimitBytes != 0 {
		duration := time.Second * 3
		if t.cfg.Options.CheckSizeDuration != 0 {
			duration = t.cfg.Options.CheckSizeDuration
		}

		sizer, err := t.getTableSizer(ctx, schema.TableName)
		if err != nil {
			return fmt.Errorf("%w: %s", err, fnName)
		}

		stopper = size.NewStopper(
			ctx, sizer, schemaAwareID, int64(target.LimitBytes),
			t.collector, duration,
		)
	}

	t.collector.RegisterTask(
		schemaAwareID.String(),
		int64(target.LimitRows),
		datasize.ByteSize(target.LimitBytes),
	)

	gens, err := t.schemaGenerators(ctx, schema, target, t.registry)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	t.tasks = append(t.tasks, model.Task{
		DatasetSchema: schema,
		Stopper:       stopper,
		Generators:    gens,
	})

	return nil
}

func (t *tableTaskBuilder) schemaGenerators(
	ctx context.Context,
	dataset model.DatasetSchema,
	target *config.Table,
	registry generatorRegistry,
) ([]model.Generator, error) {
	const fnName = "schema generators"

	userSettingsByID := make(map[model.Identifier]config.Generator)
	for _, settings := range target.Generators {
		id, err := t.schemaProvider.ColumnIdentifier(ctx, dataset.TableName, settings.Column)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, fnName)
		}

		userSettingsByID[id] = settings
	}

	generators := make([]model.Generator, 0, len(dataset.Columns))
	for _, targetType := range dataset.Columns {
		userSettings := mo.None[config.Generator]()
		if set, ok := userSettingsByID[targetType.SourceName]; ok {
			delete(userSettingsByID, targetType.SourceName)
			userSettings = mo.Some(set)
		}

		req := contract.AcceptRequest{
			Dataset:      dataset,
			UserSettings: userSettings,
			BaseType:     mo.Some(targetType),
		}

		gen, err := registry.GetGenerator(ctx, req)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: %s.%s %s",
				err,
				dataset.TableName.Quoted(),
				targetType.SourceName.AsArgument(),
				fnName,
			)
		}

		generators = append(generators, gen)
	}

	if len(userSettingsByID) > 0 {
		return nil, fmt.Errorf("unknown user config %s", fnName)
	}

	return generators, nil
}

func (t *tableTaskBuilder) getTableSizer(ctx context.Context, table model.TableName) (size.TableSizer, error) {
	const fnName = "get table sizer"

	switch t.cfg.Connection.Type {
	case config.PostgresqlConnection:
		if t.lazyCommonPool == nil {
			pool, err := pgxpool.New(ctx, t.cfg.Connection.ConnString())
			if err != nil {
				return nil, fmt.Errorf("%w: %s", err, fnName)
			}
			t.lazyCommonPool = pgxadapter.NewAdapterPool(pool)
		}

		sizer, err := postgres.NewTableSizer(ctx, t.lazyCommonPool, table)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, fnName)
		}

		return sizer, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedLimitSizerDriver, fnName)
	}
}

func (t *tableTaskBuilder) sortTasks() ([]model.Task, error) {
	byID := lo.SliceToMap(t.tasks, func(t model.Task) (model.TableName, model.Task) {
		return t.DatasetSchema.TableName, t
	})

	ids := slices.Collect(maps.Keys(byID))

	sortedIDs, err := topSort(ids, t.refresolver.DepsOn())
	if err != nil {
		return nil, fmt.Errorf("%w: sort tasks", err)
	}

	return lo.FilterMap(sortedIDs, func(t model.TableName, _ int) (model.Task, bool) {
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
