package command

import (
	"github.com/viktorkomarov/datagen/cmd/datagen/gen"

	"github.com/spf13/cobra"
)

func New() *cobra.Command {
	//nolint:exhaustruct // it's okay for now
	c := &cobra.Command{
		Use: "datagen",
	}
	c.AddCommand(gen.New())

	return c
}
