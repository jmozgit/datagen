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

func (b *BatchExecutor) Execute(ctx context.Context, task model.Task) error {
	defer func() {
		for i := range task.Generators {
			task.Generators[i].Close()
		}
	}()

	batchID := 0
	batch := make([][]any, b.cnt)
	for i := range batch {
		batch[i] = make([]any, len(task.Generators))
	}

	for {
		for i, gen := range task.Generators {
			cell, err := gen.Gen(ctx)
			if err != nil {
				return fmt.Errorf("%w: execute %s", err, task.DatasetSchema.Columns[i].SourceName.AsArgument())
			}

			batch[batchID][i] = cell
		}

		if batchID+1 == len(batch) {
			batch := model.SaveBatch{
				Schema:      task.DatasetSchema,
				Data:        batch[:batchID+1],
				SavingHints: b.saver.PrepareHints(ctx, task.DatasetSchema, task.Generators),
				Invalid:     make([]bool, b.cnt),
			}

			saved, err := b.saver.Save(ctx, batch)
			if err != nil {
				return fmt.Errorf("%w: execute %s", err, task.DatasetSchema.TableName.Quoted())
			}

			b.refNotifier.OnSaved(saved.Batch)

			cntn, err := task.Stopper.ContinueAllowed(saved.Stat)
			if err != nil {
				return fmt.Errorf("%w: execute", err)
			}

			if !cntn {
				return nil
			}
		}

		batchID = (batchID + 1) % len(batch)
	}
}
