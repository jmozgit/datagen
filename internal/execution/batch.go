package execution

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/saver/factory"
)

type refNotifier interface {
	OnSaved(batch model.SaveBatch)
}

type BatchExecutor struct {
	saver       factory.Saver
	refNotifier refNotifier
	cnt         int
}

func NewBatchExecutor(
	saver factory.Saver,
	refNotifier refNotifier,
	cnt int,
) *BatchExecutor {
	return &BatchExecutor{
		saver:       saver,
		refNotifier: refNotifier,
		cnt:         cnt,
	}
}

func shouldContinue(collected, task model.TaskProgress, buffered uint64) bool {
	return task.Rows > collected.Rows+buffered
}

func (b *BatchExecutor) Execute(ctx context.Context, task model.TaskGenerators) error {
	defer func() {
		for i := range task.Generators {
			task.Generators[i].Close()
		}
	}()

	// reuse batch
	collected := model.TaskProgress{
		Rows:  0,
		Bytes: 0,
	}
	batchID := 0
	batch := make([][]any, b.cnt)
	for i := range batch {
		batch[i] = make([]any, len(task.Generators))
	}

	for shouldContinue(collected, task.Limit, 0) {
		for i, gen := range task.Generators {
			cell, err := gen.Gen(ctx)
			if err != nil {
				return fmt.Errorf("%w: execute %s", err, task.DatasetSchema.Columns[i].SourceName)
			}

			batch[batchID][i] = cell
		}

		if batchID+1 == len(batch) || !shouldContinue(collected, task.Limit, uint64(batchID)+1) {
			batch := model.SaveBatch{
				Schema: task.DatasetSchema,
				Data:   batch[:batchID+1],
			}

			saved, err := b.saver.Save(ctx, batch)
			if err != nil {
				return fmt.Errorf("%w: execute %s", err, task.DatasetSchema.TableName)
			}

			b.refNotifier.OnSaved(batch)

			collected = model.TaskProgress{
				Bytes: collected.Bytes + uint64(saved.BytesSaved),
				Rows:  collected.Rows + uint64(saved.RowsSaved),
			}
		}
		batchID = (batchID + 1) % len(batch)
	}

	return nil
}
