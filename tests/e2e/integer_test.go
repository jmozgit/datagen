package e2e_test

import (
	"testing"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/tests/suite"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func Test_MixedIntegersFormat(t *testing.T) {
	baseSuite := suite.NewBaseSuite(t)
	table := baseSuite.NewTable("test_mixed_integers", []suite.Column{
		suite.NewColumn("integer", suite.TypeInt4),
		suite.NewColumn("serial", suite.TypeSerialInt4),
	})
	baseSuite.CreateTable(table)

	baseSuite.SaveConfig(
		suite.WithBatchSize(3),
		//nolint:exhaustruct // ok
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			Generators: []config.Generator{},
			LimitRows:  2,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		cnt++
		require.Len(t, row, len(table.Columns))
		for _, val := range row {
			_ = toInteger(t, val)
		}
	})
	require.GreaterOrEqual(t, cnt, 2)
}

func Test_PostgresqlAllIntegers(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	baseSuite := suite.NewBaseSuite(t)
	table := baseSuite.NewTable("test_all_integers", []suite.Column{
		suite.NewColumnRawType("smallint", "smallint"),
		suite.NewColumnRawType("integer", "integer"),
		suite.NewColumnRawType("bigint", "bigint"),
		suite.NewColumnRawType("int2", "int2"),
		suite.NewColumnRawType("int4", "int4"),
		suite.NewColumnRawType("int8", "int8"),
	})
	baseSuite.CreateTable(table)
	baseSuite.SaveConfig(
		suite.WithBatchSize(7),
		//nolint:exhaustruct // ok
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			Generators: []config.Generator{},
			LimitRows:  39,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		cnt++
		require.Len(t, row, len(table.Columns))
		for _, val := range row {
			_ = toInteger(t, val)
		}
	})
	require.GreaterOrEqual(t, cnt, 39)
}

func Test_SerialPostgresqlDefault(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	baseSuite := suite.NewBaseSuite(t)
	table := baseSuite.NewTable("test_default_serial", []suite.Column{
		suite.NewColumnRawType("smallserial", "smallserial"),
		suite.NewColumnRawType("serial", "serial"),
		suite.NewColumnRawType("bigserial", "bigserial"),
	})

	baseSuite.CreateTable(table)
	baseSuite.SaveConfig(
		suite.WithBatchSize(11),
		//nolint:exhaustruct // ok
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			Generators: []config.Generator{},
			LimitRows:  12,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0

	curValues := []int64{0, 0, 0}
	baseSuite.OnEachRow(table, func(row []any) {
		require.Len(t, row, len(table.Columns))
		for i, val := range row {
			v := toInteger(t, val)
			if cnt != 0 {
				require.Equal(t, curValues[i]+1, v)
			}
			curValues[i] = v
		}
		cnt++
	})
	require.GreaterOrEqual(t, cnt, 12)
}

func Test_SerialGeneratorFromConfig(t *testing.T) {
	baseSuite := suite.NewBaseSuite(t)
	table := baseSuite.NewTable("test_serial", []suite.Column{
		suite.NewColumn("smallserial", suite.TypeSerialInt2),
		suite.NewColumn("serial", suite.TypeSerialInt4),
		suite.NewColumn("bigserial", suite.TypeSerialInt8),
	})

	minValues := [3]int64{-10, 5, 0}

	baseSuite.CreateTable(table)
	baseSuite.SaveConfig(
		suite.WithBatchSize(13),
		//nolint:exhaustruct // ok
		suite.WithTableTarget(config.Table{
			Schema: table.Schema,
			Table:  table.Name,
			Generators: []config.Generator{
				{
					Column: "smallserial",
					Type:   config.GeneratorTypeInteger,
					Integer: &config.Integer{
						Format:   lo.ToPtr("serial"),
						MinValue: lo.ToPtr(minValues[0]),
					},
				},
				{
					Column: "serial",
					Type:   config.GeneratorTypeInteger,
					Integer: &config.Integer{
						Format:   lo.ToPtr("serial"),
						MinValue: lo.ToPtr(minValues[1]),
					},
				},
				{
					Column: "bigserial",
					Type:   config.GeneratorTypeInteger,
					Integer: &config.Integer{
						Format:   lo.ToPtr("serial"),
						MinValue: lo.ToPtr(minValues[2]),
					},
				},
			},
			LimitRows: 56,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		require.Len(t, row, len(table.Columns))
		for i, val := range row {
			v := toInteger(t, val)
			require.Equal(t, minValues[i]+int64(cnt), v)
		}
		cnt++
	})
	require.GreaterOrEqual(t, cnt, 56)
}

func Test_IntegerGeneratorRespectConstraints(t *testing.T) {
	baseSuite := suite.NewBaseSuite(t)

	table := baseSuite.NewTable("test_integer_respect_constraints",
		[]suite.Column{
			suite.NewColumn("gen_col", suite.TypeInt4),
		})

	baseSuite.CreateTable(table)

	baseSuite.SaveConfig(
		suite.WithBatchSize(1),
		//nolint:exhaustruct // ok for tests
		suite.WithTableTarget(config.Table{
			Schema:    table.Schema,
			Table:     table.Name,
			LimitRows: 150,
			Generators: []config.Generator{
				{
					Type:   "integer",
					Column: table.Columns[0].Name,
					Integer: &config.Integer{
						Format:   lo.ToPtr("random"),
						ByteSize: lo.ToPtr[int8](4),
						MinValue: lo.ToPtr[int64](-10),
						MaxValue: lo.ToPtr[int64](98),
					},
				},
			},
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		cnt++
		require.Len(t, row, 1)
		number := toInteger(t, row[0])
		require.True(t, number >= -10 && number <= 98)
	})
	require.GreaterOrEqual(t, cnt, 150)
}

func toInteger(t *testing.T, val any) int64 {
	t.Helper()

	switch v := val.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case uint:
		return int64(v)
	default:
		require.Failf(t, "integer mismatched", "expected integer type, not %T (%v)", val, val)

		return 0
	}
}
