package e2e_test

import (
	"testing"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/tests/suite"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func Test_PostgresqlAllIntegers(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	baseSuite := suite.NewBaseSuite(t)
	table := model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_all_integers",
		},
		Columns: []model.Column{
			{Name: "smallint", Type: "smallint", IsNullable: false, FixedSize: 2},
			{Name: "integer", Type: "integer", IsNullable: false, FixedSize: 4},
			{Name: "bigint", Type: "bigint", IsNullable: false, FixedSize: 8},
			{Name: "int2", Type: "int2", IsNullable: false, FixedSize: 2},
			{Name: "int4", Type: "int4", IsNullable: false, FixedSize: 4},
			{Name: "int8", Type: "int8", IsNullable: false, FixedSize: 8},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	}
	baseSuite.CreateTable(table, options.WithPreserve())
	baseSuite.SaveConfig(
		suite.WithBatchSize(7),
		//nolint:exhaustruct // ok
		suite.WithTableTarget(config.Table{
			Schema:     string(table.Name.Schema),
			Table:      string(table.Name.Table),
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
	require.Equal(t, 39, cnt)
}

func Test_IntegerGeneratorRespectConstraints(t *testing.T) {
	baseSuite := suite.NewBaseSuite(t)

	table := model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_integer_respect_constraints",
		},
		Columns: []model.Column{
			{
				Name:       "gen_col",
				Type:       "integer",
				IsNullable: false,
				FixedSize:  4,
			},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	}

	baseSuite.CreateTable(table)

	baseSuite.SaveConfig(
		suite.WithBatchSize(1),
		//nolint:exhaustruct // ok for tests
		suite.WithTableTarget(config.Table{
			Schema:    string(table.Name.Schema),
			Table:     string(table.Name.Table),
			LimitRows: 150,
			Generators: []config.Generator{
				{
					Type:   "integer",
					Column: string(table.Columns[0].Name),
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
	require.Equal(t, 150, cnt)
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
