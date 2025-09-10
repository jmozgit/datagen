package workmanager

import (
	"context"
	"fmt"
	"sync"

	"github.com/viktorkomarov/datagen/internal/model"
)

type Saver interface {
	Save(ctx context.Context, schema model.DatasetSchema, data []any) model.SaveReport
}

type GenPicker interface {
	PickGenerator(ctx context.Context, req any) (model.Generator, error)
}

type Manager struct {
	picker    GenPicker
	saver     Saver
	workerCnt int
}

func (m *Manager) pickGeneratorsForSchema(ctx context.Context, schema model.DatasetSchema) ([]model.Generator, error) {
	gens := make([]model.Generator, len(schema.DataTypes))
	for i, cellType := range schema.DataTypes {
		gen, err := m.picker.PickGenerator(ctx, cellType)
		if err != nil {
			return nil, fmt.Errorf("%w: pick generators for type %s", err, cellType)
		}
		gens[i] = gen
	}

	return gens, nil
}

func (m *Manager) Execute(ctx context.Context, tasks []model.Task) error {
	taskGens := make([]model.TaskGenerators, len(tasks))
	for i, task := range tasks {
		gen, err := m.pickGeneratorsForSchema(ctx, task.Schema)
		if err != nil {
			return fmt.Errorf("%w: execute", err)
		}
		taskGens[i] = model.TaskGenerators{
			Task:       task,
			Generators: gen,
		}
	}

	taskCh := make(chan model.TaskGenerators)
	stopAndWaitWorkers := m.startWorkers(ctx, taskCh)
	defer stopAndWaitWorkers()

	for _, task := range taskGens {
		select {
		case <-ctx.Done():
			return nil
		case taskCh <- task:
		}
	}

	return nil
}

func (m *Manager) startWorkers(ctx context.Context, taskCh <-chan model.TaskGenerators) func() {
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(ctx)
	for range m.workerCnt {
		wg.Add(1)
		go func() {
			defer wg.Done()

			m.work(ctx, taskCh)
		}()
	}
	return func() {
		cancel()
		wg.Wait()
	}
}

func (m *Manager) work(ctx context.Context, tasks <-chan model.TaskGenerators) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-tasks:
			m.executeTask(ctx, task)
		}
	}
}

func (m *Manager) executeTask(ctx context.Context, task model.TaskGenerators) error {
	collected := model.TaskProgress{
		Bytes: 0, Rows: 0,
	}
	shouldContinue := func() bool {
		return !(task.Limit.Bytes > collected.Bytes && task.Limit.Rows > collected.Rows)
	}

	for shouldContinue() {
		data := make([]any, len(task.Generators))

		for i, gen := range task.Generators {
			cell, err := gen.Gen(ctx)
			if err != nil {
				return fmt.Errorf("%w: generate for %s", err, task.Schema.DataTypes[i])
			}
			data[i] = cell
		}

		saved := m.saver.Save(ctx, task.Schema, data)
		collected = model.TaskProgress{
			Bytes: collected.Bytes + uint64(saved.BytesSaved),
			Rows:  collected.Rows + uint64(saved.RowsSaved),
		}
	}

	return nil
}
