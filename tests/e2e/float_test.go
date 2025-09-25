package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/tests/suite"
)

func Test_FloatGeneratorFromType(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	baseSuite := suite.NewBaseSuite(t)
	table := suite.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_float32",
		},
		Columns: []suite.Column{
			{Name: "real", Type: suite.TypeFloat4},
			{Name: "double", Type: suite.TypeFloat8},
		},
	}
	baseSuite.CreateTable(table, options.WithPreserve())

	baseSuite.SaveConfig(
		suite.WithBatchSize(17),
		//nolint:exhaustruct // ok
		suite.WithTableTarget(config.Table{
			Schema:     string(table.Name.Schema),
			Table:      string(table.Name.Table),
			Generators: []config.Generator{},
			LimitRows:  35,
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
