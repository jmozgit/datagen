package gen

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/jmozgit/datagen/internal/acceptor/registry"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/execution"
	"github.com/jmozgit/datagen/internal/pkg/closer"
	"github.com/jmozgit/datagen/internal/progress"
	"github.com/jmozgit/datagen/internal/progress/terminal"
	"github.com/jmozgit/datagen/internal/refresolver"
	"github.com/jmozgit/datagen/internal/saver/factory"

	"github.com/spf13/cobra"
)

type cmd struct {
	flags              flags
	cfg                config.Config
	closer             *closer.Registry
	refSvc             *refresolver.Service
	acceptors          *registry.Acceptors
	saver              factory.Saver
	taskExecutor       *execution.BatchExecutor
	progressController *progress.Controller
}

type flags struct {
	path    string
	workCnt int
}

func New() *cobra.Command {
	cmd := new(cmd)

	var flags flags

	//nolint:exhaustruct // it's okay for now
	c := &cobra.Command{
		Use:   "gen",
		Short: "generate data",
		PreRunE: func(cobraCmd *cobra.Command, _ []string) error {
			if err := cmd.init(cobraCmd.Context(), flags); err != nil {
				return fmt.Errorf("%w: pre run", err)
			}

			return nil
		},
		RunE: func(cobraCmd *cobra.Command, args []string) error {
			if err := cmd.exec(cobraCmd, args); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}

				return err
			}

			return nil
		},
		PostRunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			return cmd.closer.CloseAll(ctx)
		},
	}

	parseFlags(c, &flags)

	return c
}

func (c *cmd) init(ctx context.Context, flags flags) error {
	const fnName = "init"

	var err error
	c.flags = flags
	c.cfg, err = config.Load(flags.path)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}
	c.closer = closer.NewRegistry()
	c.refSvc = refresolver.NewService()
	c.acceptors, err = registry.PrepareAcceptors(ctx, c.cfg, c.refSvc, c.closer)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}
	c.saver, err = factory.GetSaver(ctx, c.cfg)
	if err != nil {
		return fmt.Errorf("%w: %s", err, fnName)
	}
	c.taskExecutor = execution.NewBatchExecutor(c.saver, c.refSvc, c.cfg.Options.BatchSize)
	terminal := terminal.New(os.Stdout)
	c.progressController = progress.NewController(terminal, flags.workCnt)
	c.closer.Add(closer.Fn(c.progressController.Close))

	go c.progressController.Run(ctx)

	return nil
}

func parseFlags(rootCmd *cobra.Command, flags *flags) {
	rootCmd.PersistentFlags().StringVarP(&flags.path, "config", "f", "config.yaml", "path to config file")
	rootCmd.PersistentFlags().IntVarP(&flags.workCnt, "workers", "w", runtime.NumCPU(), "count of parallel workers")
}
