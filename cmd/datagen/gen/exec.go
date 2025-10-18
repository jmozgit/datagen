package gen

import (
	"fmt"

	"github.com/viktorkomarov/datagen/internal/taskbuilder"

	"github.com/spf13/cobra"
)

func (c *cmd) exec(cmd *cobra.Command, _ []string) error {
	const fnName = "exec"

	ctx := cmd.Context()

	tasks, err := taskbuilder.Build(ctx, c.cfg, c.acceptors, c.refSvc)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	if err := c.workmanager.Execute(ctx, tasks); err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}

	return nil
}
