package e2e_test

import (
	"testing"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/tests/suite"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func Test_PostgresqlNumeric(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	baseSuite := suite.NewBaseSuite(t)
	table := baseSuite.NewTable("test_pg_numeric", []suite.Column{
		suite.NewColumnRawType("num1", "numeric(5, -3)"),
		suite.NewColumnRawType("num2", "numeric"),
		suite.NewColumnRawType("dec1", "decimal(2, 10)"),
		suite.NewColumnRawType("dec2", "decimal"),
	})
	baseSuite.CreateTable(table)

	baseSuite.SaveConfig(
		suite.WithBatchSize(15),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			Generators: []config.Generator{},
			LimitRows:  15,
			LimitBytes: 0,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	checkValuesAreFloat(t, baseSuite, table, 15)
}

func Test_FloatGeneratorFromType(t *testing.T) {
	baseSuite := suite.NewBaseSuite(t)
	table := baseSuite.NewTable("test_float", []suite.Column{
		suite.NewColumn("real", suite.TypeFloat4),
		suite.NewColumn("double", suite.TypeFloat8),
	})
	baseSuite.CreateTable(table)

	baseSuite.SaveConfig(
		suite.WithBatchSize(17),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			Generators: []config.Generator{},
			LimitRows:  35,
			LimitBytes: 0,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	checkValuesAreFloat(t, baseSuite, table, 35)
}

func checkValuesAreFloat(t *testing.T, baseSuite *suite.BaseSuite, table suite.Table, expectedCnt int) {
	t.Helper()

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		cnt++
		require.Len(t, row, len(table.Columns))
		for _, val := range row {
			_ = toFloat(t, val)
		}
	})
	require.Equal(t, expectedCnt, cnt)
}

func toFloat(t *testing.T, val any) float64 {
	t.Helper()

	switch v := val.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	case pgtype.Numeric:
		vf, err := v.Float64Value()
		require.NoError(t, err)

		return vf.Float64
	default:
		require.Failf(t, "float mismatched", "expected float type, not %T (%v)", val, val)

		return 0
	}
}
