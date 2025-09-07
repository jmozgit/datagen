package gen

import "github.com/spf13/cobra"

type cmd struct {
}

func New() *cobra.Command {
	cmd := &cmd{}

	c := &cobra.Command{
		Use:   "datagen gen -f config.yaml",
		Short: "generate data",
		RunE:  cmd.exec,
	}

	return c
}
