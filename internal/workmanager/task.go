package workmanager

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/model"
)

func GenerateForTask(ctx context.Context, task model.TaskGenerators, saver Saver) error {
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

		saved := saver.Save(ctx, task.Schema, data)
		collected = model.TaskProgress{
			Bytes: collected.Bytes + uint64(saved.BytesSaved),
			Rows:  collected.Rows + uint64(saved.RowsSaved),
		}
	}

	return nil
}
