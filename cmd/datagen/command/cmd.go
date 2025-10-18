package command

import (
	"context"

	"github.com/viktorkomarov/datagen/cmd/datagen/gen"

	"github.com/spf13/cobra"
)

func New(ctx context.Context) *cobra.Command {
	//nolint:exhaustruct // it's okay for now
	c := &cobra.Command{
		Use: "datagen",
	}
	c.SetContext(ctx)

	c.AddCommand(gen.New())

	return c
}
