package execution

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/saver/factory"
)

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

func shouldContinue(collected, task model.TaskProgress, buffered uint64) bool {
	return task.Rows > collected.Rows+buffered
}

func (b *BatchExecutor) Execute(ctx context.Context, task model.TaskGenerators) error {
	for i := range b.batch {
		if len(b.batch[i]) == 0 {
			b.batch[i] = make([]any, len(task.Generators))
		}
	}

	for shouldContinue(b.collected, task.Limit, 0) {
		for i, gen := range task.Generators {
			cell, err := gen.Gen(ctx)
			if err != nil {
				return fmt.Errorf("%w: execute %s", err, task.Schema.DataTypes[i])
			}
			b.batch[b.batchID][i] = cell
		}

		if b.batchID+1 == len(b.batch) || !shouldContinue(b.collected, task.Limit, uint64(b.batchID)+1) {
			saved, err := b.saver.Save(ctx, model.SaveBatch{
				Schema: task.Schema,
				Data:   b.batch[:b.batchID+1],
			})
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
