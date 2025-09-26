package e2e_test

import (
	"testing"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/tests/suite"

	"github.com/stretchr/testify/require"
)

func Test_FloatGeneratorFromType(t *testing.T) {
	baseSuite := suite.NewBaseSuite(t)
	table := suite.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_float",
		},
		Columns: []suite.Column{
			suite.NewColumn("real", suite.TypeFloat4),
			suite.NewColumn("double", suite.TypeFloat8),
		},
	}
	baseSuite.CreateTable(table)

	baseSuite.SaveConfig(
		suite.WithBatchSize(17),
		suite.WithTableTarget(config.Table{
			Schema:     string(table.Name.Schema),
			Table:      string(table.Name.Table),
			Generators: []config.Generator{},
			LimitRows:  35,
			LimitBytes: 0,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		cnt++
		require.Len(t, row, len(table.Columns))
		for _, val := range row {
			_ = toFloat(t, val)
		}
	})
	require.Equal(t, 35, cnt)
}

func toFloat(t *testing.T, val any) float64 {
	t.Helper()

	switch v := val.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		require.Failf(t, "float mismatched", "expected float type, not %T (%v)", val, val)

		return 0
	}
}
