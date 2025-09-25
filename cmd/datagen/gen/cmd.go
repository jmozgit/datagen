package gen

import (
	"context"
	"fmt"
	"runtime"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/pkg/closer"

	"github.com/spf13/cobra"
)

type cmd struct {
	flags          flags
	cfg            config.Config
	closerRegistry *closer.Registry
}

type flags struct {
	path    string
	workCnt int
}

func New() *cobra.Command {
	cmd := new(cmd)

	//nolint:exhaustruct // it's okay for now
	c := &cobra.Command{
		Use:   "gen",
		Short: "generate data",
		PreRunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load(cmd.flags.path)
			if err != nil {
				return fmt.Errorf("%w: pre run", err)
			}
			cmd.cfg = cfg
			cmd.closerRegistry = closer.NewRegistry()

			return nil
		},
		RunE: cmd.exec,
		PostRunE: func(_ *cobra.Command, _ []string) error {
			// change context background
			return cmd.closerRegistry.Close(context.Background())
		},
	}

	parseFlags(c, &cmd.flags)

	return c
}

func parseFlags(rootCmd *cobra.Command, flags *flags) {
	rootCmd.PersistentFlags().StringVarP(&flags.path, "config", "f", "config.yaml", "path to config file")
	rootCmd.PersistentFlags().IntVarP(&flags.workCnt, "workers", "w", runtime.NumCPU(), "count of parallel workers")
}
