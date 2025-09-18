package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/saver/factory"
)

var ErrTaskIsExecuted = errors.New("task is executed")

type Saver interface {
	factory.Saver
}

type BatchExecutor struct {
	saver     Saver
	collected model.TaskProgress
	batchID   int
	batch     [][]any
}

func NewBatchExecutor(saver Saver, cnt int) *BatchExecutor {
	return &BatchExecutor{
		saver:   saver,
		batchID: 0,
		collected: model.TaskProgress{
			Bytes: 0,
			Rows:  0,
		},
		batch: make([][]any, cnt),
	}
}

func shouldContinue(collected, task model.TaskProgress) bool {
	return task.Bytes > collected.Bytes || task.Rows > collected.Rows
}

func (b *BatchExecutor) Execute(ctx context.Context, task model.TaskGenerators) error {
	for i := range task.Generators {
		if len(b.batch[i]) == 0 {
			b.batch[i] = make([]any, len(task.Generators))
		}
	}

	for shouldContinue(b.collected, task.Limit) {
		for i, gen := range task.Generators {
			cell, err := gen.Gen(ctx)
			if err != nil {
				return fmt.Errorf("%w: execute %s", err, task.Schema.DataTypes[i])
			}
			b.batch[b.batchID][i] = cell
		}

		if b.batchID+1 == len(b.batch) {
			saved, err := b.saver.Save(ctx, task.Schema, b.batch)
			if err != nil {
				return fmt.Errorf("%w: execute", err)
			}

			b.collected = model.TaskProgress{
				Bytes: b.collected.Bytes + uint64(saved.BytesSaved),
				Rows:  b.collected.Rows + uint64(saved.RowsSaved),
			}
		}
		b.batchID = (b.batchID + 1) % len(b.batch)
	}

	return nil
}
