package gen

import (
	"fmt"
	"runtime"

	"github.com/viktorkomarov/datagen/internal/config"

	"github.com/spf13/cobra"
)

type cmd struct {
	flags flags
	cfg   config.Config
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

			return nil
		},
		RunE: cmd.exec,
	}

	parseFlags(c, &cmd.flags)

	return c
}

func parseFlags(rootCmd *cobra.Command, flags *flags) {
	rootCmd.PersistentFlags().StringVarP(&flags.path, "config", "f", "config.yaml", "path to config file")
	rootCmd.PersistentFlags().IntVarP(&flags.workCnt, "workers", "w", runtime.NumCPU(), "count of parallel workers")
}
