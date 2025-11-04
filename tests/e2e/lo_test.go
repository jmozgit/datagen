package e2e_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/pkg/db"
	"github.com/jmozgit/datagen/internal/pkg/testconn/options"
	"github.com/jmozgit/datagen/tests/suite"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PostgresqlOID(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)

	table := bs.NewTable("oid_test",
		[]suite.Column{
			suite.NewColumnRawType("id", "int"),
			suite.NewColumnRawType("blob", "oid"),
		},
	)
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(7),
		suite.WithTableTarget(config.Table{
			Schema:    table.Schema,
			Table:     table.Name,
			LimitRows: 59,
			Generators: []config.Generator{
				{
					Column: "blob",
					Type:   config.GeneratorTypeLO,
					LO: &config.LO{
						Size:  datasize.KB * 10,
						Range: datasize.KB * 1,
					},
				},
			},
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0

	checkOID := make([]uint32, 0)
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 2)

		oid := toInteger(t, row[1])
		checkOID = append(checkOID, uint32(oid))
		cnt++
	})
	require.GreaterOrEqual(t, cnt, 59)

	for _, oid := range checkOID {
		bs.ExecuteInFunc(func(ctx context.Context, c db.Connect) error {
			var sum int64
			err := c.QueryRow(ctx, "SELECT SUM(LENGTH(data)) FROM pg_largeobject WHERE loid = $1", oid).Scan(&sum)
			if err != nil {
				return err
			}

			assert.True(t, int64(datasize.KB*10-datasize.KB*1) <= sum && sum <= int64(datasize.KB*10+datasize.KB*1))

			return nil
		})
	}
}

func Test_DeleteLOIfNoProgress(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)
	table := bs.NewTable("oid_test",
		[]suite.Column{
			suite.NewColumnRawType("id", "int"),
			suite.NewColumnRawType("blob", "oid"),
		},
	)
	bs.CreateTable(table, options.WithPKs([]string{"id"}), options.WithPreserve())

	bs.ExecuteInFunc(func(ctx context.Context, c db.Connect) error {
		err := c.Execute(ctx, "INSERT INTO oid_test (id) SELECT * FROM generate_series(1, 10)")
		if err != nil {
			return fmt.Errorf("%w: execute in func", err)
		}

		return nil
	})

	bs.SaveConfig(
		suite.WithBatchSize(7),
		suite.WithNoAttemptsProgress(10),
		suite.WithTableTarget(config.Table{
			Schema:    table.Schema,
			Table:     table.Name,
			LimitRows: 10,
			Generators: []config.Generator{
				{
					Column: "blob",
					Type:   config.GeneratorTypeLO,
					LO: &config.LO{
						Size:  datasize.KB * 5,
						Range: datasize.KB * 1,
					},
				},
				{
					Column: "id",
					Type:   config.GeneratorTypeInteger,
					Integer: &config.Integer{
						Format:   lo.ToPtr("random"),
						ByteSize: lo.ToPtr[int8](4),
						MinValue: lo.ToPtr[int64](1),
						MaxValue: lo.ToPtr[int64](10),
					},
				},
			},
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.Error(t, err)

	bs.ExecuteInFunc(func(ctx context.Context, c db.Connect) error {
		var cnt int
		err := c.QueryRow(ctx, "SELECT count(*) FROM pg_largeobject").Scan(&cnt)
		if err != nil {
			return fmt.Errorf("%w: execute in func", err)
		}
		require.Equal(t, cnt, 0)

		err = c.QueryRow(ctx, "SELECT count(*) FROM oid_test").Scan(&cnt)
		if err != nil {
			return fmt.Errorf("%w: execute in func", err)
		}
		require.Equal(t, cnt, 10)

		return nil
	})
}

func Test_PostgresqlOID2Gb(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)

	table := bs.NewTable("oid_long",
		[]suite.Column{
			suite.NewColumnRawType("id", "int"),
			suite.NewColumnRawType("blob", "oid"),
		},
	)
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(1),
		suite.WithTableTarget(config.Table{
			Schema:    table.Schema,
			Table:     table.Name,
			LimitRows: 1,
			Generators: []config.Generator{
				{
					Column: "blob",
					Type:   config.GeneratorTypeLO,
					LO: &config.LO{
						Size: datasize.GB * 1,
					},
				},
			},
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0

	checkOID := make([]uint32, 0)
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 2)

		oid := toInteger(t, row[1])
		checkOID = append(checkOID, uint32(oid))
		cnt++
	})
	require.GreaterOrEqual(t, cnt, 1)

	for _, oid := range checkOID {
		bs.ExecuteInFunc(func(ctx context.Context, c db.Connect) error {
			var sum int64
			err := c.QueryRow(ctx, "SELECT SUM(LENGTH(data)) FROM pg_largeobject WHERE loid = $1", oid).Scan(&sum)
			if err != nil {
				return err
			}

			assert.Equal(t, int64(datasize.GB), sum)

			return nil
		})
	}
}
