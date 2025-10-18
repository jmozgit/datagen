package gen

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/viktorkomarov/datagen/internal/acceptor/registry"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/execution"
	"github.com/viktorkomarov/datagen/internal/pkg/closer"
	"github.com/viktorkomarov/datagen/internal/refresolver"
	"github.com/viktorkomarov/datagen/internal/saver/factory"
	"github.com/viktorkomarov/datagen/internal/workmanager"

	"github.com/spf13/cobra"
)

type cmd struct {
	cfg         config.Config
	closer      *closer.Registry
	refSvc      *refresolver.Service
	acceptors   *registry.Acceptors
	saver       factory.Saver
	executor    *execution.BatchExecutor
	workmanager *workmanager.Manager
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
		RunE: cmd.exec,
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
	c.executor = execution.NewBatchExecutor(c.saver, c.refSvc, c.cfg.Options.BatchSize)
	c.workmanager = workmanager.New(flags.workCnt, c.executor.Execute)

	return nil
}

func parseFlags(rootCmd *cobra.Command, flags *flags) {
	rootCmd.PersistentFlags().StringVarP(&flags.path, "config", "f", "config.yaml", "path to config file")
	rootCmd.PersistentFlags().IntVarP(&flags.workCnt, "workers", "w", runtime.NumCPU(), "count of parallel workers")
}
