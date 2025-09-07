package command

import (
	"github.com/spf13/cobra"
	"github.com/viktorkomarov/datagen/cmd/datagen/gen"
)

func New() *cobra.Command {
	c := &cobra.Command{
		Use: "datagen",
	}
	c.AddCommand(gen.New())

	return c
}
