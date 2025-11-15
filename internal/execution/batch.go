package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/saver/factory"
)

var ErrNoProgressHappens = errors.New("no progress happens")

type refNotifier interface {
	OnProcessed(batch model.SaveBatch)
}

type BatchExecutor struct {
	saver              factory.Saver
	refNotifier        refNotifier
	batchSize          int
	noProgressAttempts int
}

func NewBatchExecutor(
	saver factory.Saver,
	refNotifier refNotifier,
	batchSize int,
	noProgressAttempts int,
) *BatchExecutor {
	return &BatchExecutor{
		saver:              saver,
		refNotifier:        refNotifier,
		batchSize:          batchSize,
		noProgressAttempts: noProgressAttempts,
	}
}

func (b *BatchExecutor) Execute(ctx context.Context, task model.Task) error {
	defer func() {
		for i := range task.Generators {
			task.Generators[i].Close()
		}
	}()

	batch := make([][]any, b.batchSize)
	for i := range batch {
		batch[i] = make([]any, len(task.Generators))
	}

	noProgressLoop := 0
	for {
		nextTicket, err := task.Limiter.NextTicket(ctx, int64(b.batchSize))
		if err != nil {
			return fmt.Errorf("%w: execute", err)
		}
		rows := nextTicket.AllowedRows

		if rows == 0 {
			return nil
		}

		batchID := 0
		for rows > int64(0) {
			for i, gen := range task.Generators {
				cell, err := gen.Gen(ctx)
				if err != nil {
					return fmt.Errorf("%w: execute %s", err, task.DatasetSchema.Columns[i].SourceName.AsArgument())
				}

				batch[batchID][i] = cell
			}
			rows--
			batchID++
		}

		batch := model.SaveBatch{
			Schema:      task.DatasetSchema,
			Data:        batch[:batchID],
			SavingHints: b.saver.PrepareHints(ctx, task.DatasetSchema),
			Invalid:     make([]bool, b.batchSize),
		}

		saved, err := b.saver.Save(ctx, batch)
		if err != nil {
			return fmt.Errorf("%w: execute %s", err, task.DatasetSchema.TableName.Quoted())
		}
		b.refNotifier.OnProcessed(saved.Batch)

		if saved.Stat.RowsSaved == 0 {
			noProgressLoop++
		} else {
			noProgressLoop = 0
		}

		if b.noProgressAttempts != 0 && b.noProgressAttempts == noProgressLoop {
			return fmt.Errorf("%w: execute", ErrNoProgressHappens)
		}

		task.Limiter.Collect(ctx, saved.Stat)
	}
}
