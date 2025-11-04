package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
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

	threshold := datasize.KB * 350
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

func Test_LimitByTableSizeWithLOObjects(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)

	table := bs.NewTable("size_lo",
		[]suite.Column{
			suite.NewColumnRawType("oid", "oid"),
		})
	bs.CreateTable(table)

	threshold := datasize.KB * 350
	bs.SaveConfig(
		suite.WithBatchSize(100),
		suite.WithCheckTableSize(time.Millisecond*250),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitBytes: threshold,
			Generators: []config.Generator{
				{
					Column: "oid",
					Type:   config.GeneratorTypeLO,
					LO: &config.LO{
						Size:  datasize.KB * 10,
						Range: 0,
					},
				},
			},
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	var actualSize int64
	bs.ExecuteInFunc(func(ctx context.Context, c db.Connect) error {
		err := c.QueryRow(ctx, "select pg_table_size('size_lo')").Scan(&actualSize)
		if err != nil {
			return fmt.Errorf("%w: get table size", err)
		}

		var loObjectSizes int64
		err = c.QueryRow(ctx, "select sum(length(lo.data)) from pg_largeobject lo").Scan(&loObjectSizes)
		if err != nil {
			return fmt.Errorf("%s: lo_objects", err)
		}
		actualSize += loObjectSizes

		return nil
	})

	require.GreaterOrEqual(t, actualSize, int64(threshold))
}
