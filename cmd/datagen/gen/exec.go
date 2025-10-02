package gen

import (
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/registry"
	"github.com/viktorkomarov/datagen/internal/execution"
	"github.com/viktorkomarov/datagen/internal/saver/factory"
	"github.com/viktorkomarov/datagen/internal/taskbuilder"
	"github.com/viktorkomarov/datagen/internal/workmanager"

	"github.com/spf13/cobra"
)

func (c *cmd) exec(cmd *cobra.Command, _ []string) error {
	const fnName = "exec"

	ctx := cmd.Context()

	reg, err := registry.PrepareRegistry(ctx, c.cfg, c.closerRegistry)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	tasks, err := taskbuilder.Build(ctx, c.cfg, reg)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	saver, err := factory.GetSaver(ctx, c.cfg)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	batchExecutor := execution.NewBatchExecutor(saver, c.cfg.Options.BatchSize)

	manager := workmanager.New(c.flags.workCnt, batchExecutor.Execute)
	if err := manager.Execute(ctx, tasks); err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	return nil
}
