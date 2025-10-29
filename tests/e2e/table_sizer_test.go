package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/pkg/db"
	"github.com/jmozgit/datagen/tests/suite"
	"github.com/stretchr/testify/require"
)

func Test_LimitByTableSize(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)

	table := bs.NewTable("size",
		[]suite.Column{
			suite.NewColumn("comment", suite.TypeText),
			suite.NewColumn("id", suite.TypeSerialInt4),
			suite.NewColumn("created_at", suite.TypeTimestamp),
			suite.NewColumnRawType("uuid", "uuid"),
		})
	bs.CreateTable(table)

	threshold := units.KiB * 350
	bs.SaveConfig(
		suite.WithBatchSize(100),
		suite.WithCheckTableSize(time.Millisecond*250),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitBytes: threshold,
			Generators: make([]config.Generator, 0),
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	var actualSize int64
	bs.ExecuteInFunc(func(ctx context.Context, c db.Connect) error {
		err := c.QueryRow(ctx, "select pg_table_size('size')").Scan(&actualSize)
		if err != nil {
			return fmt.Errorf("%w: get table size", err)
		}

		return nil
	})

	require.GreaterOrEqual(t, actualSize, int64(threshold))
}
