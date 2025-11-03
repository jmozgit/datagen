package gen

import (
	"fmt"

	"github.com/jmozgit/datagen/internal/taskbuilder"
	"github.com/jmozgit/datagen/internal/workmanager"

	"github.com/spf13/cobra"
)

func (c *cmd) exec(cmd *cobra.Command, _ []string) error {
	const fnName = "exec"

	ctx := cmd.Context()

	tasks, err := taskbuilder.Build(
		ctx, c.cfg, c.acceptors,
		c.refSvc, c.progressController, c.closer,
	)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	wm := workmanager.New(
		min(len(tasks), c.flags.workCnt),
		c.taskExecutor.Execute,
	)

	if err := wm.Execute(ctx, tasks); err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	return nil
}
