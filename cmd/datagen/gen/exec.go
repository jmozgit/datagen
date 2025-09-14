package gen

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/viktorkomarov/datagen/internal/execution"
	"github.com/viktorkomarov/datagen/internal/generator/registry"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/saver/factory"
	"github.com/viktorkomarov/datagen/internal/taskbuilder"
	"github.com/viktorkomarov/datagen/internal/workmanager"
)

func (c *cmd) exec(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	registry := registry.New()
	tasks, err := taskbuilder.Build(ctx, c.cfg, registry)
	if err != nil {
		return fmt.Errorf("%w: exec", err)
	}

	saver, err := factory.GetSaver(ctx, c.cfg)
	if err != nil {
		return fmt.Errorf("%w: exec", err)
	}

	manager := workmanager.New(
		c.flags.workCnt,
		func(ctx context.Context, task model.TaskGenerators) error {
			return execution.TaskGeneration(ctx, task, saver)
		},
	)

	if err := manager.Execute(ctx, tasks); err != nil {
		return fmt.Errorf("%w: exec", err)
	}

	return nil
}
