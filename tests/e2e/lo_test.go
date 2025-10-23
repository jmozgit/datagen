package e2e_test

import (
	"context"
	"testing"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
	"github.com/viktorkomarov/datagen/tests/suite"
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
						Size:  units.Base2Bytes(units.KB) * 10,
						Range: units.Base2Bytes(units.KB) * 1,
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

			assert.True(t, int64(units.Base2Bytes(units.KB)*10-units.Base2Bytes(units.KB)*1) <= sum && sum <= int64(units.Base2Bytes(units.KB)*10+units.Base2Bytes(units.KB)*1))

			return nil
		})
	}
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
						Size: units.Base2Bytes(units.GB) * 1,
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

			assert.Equal(t, int64(units.Base2Bytes(units.GB)), sum)

			return nil
		})
	}
}
