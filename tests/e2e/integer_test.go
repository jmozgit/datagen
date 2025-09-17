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

//nolint:paralleltest // change it globally later
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
			},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	}

	baseSuite.CreateTable(table, options.WithPreserve())

	baseSuite.SaveConfig(
		suite.WithBatchSize(1),
		//nolint:exhaustruct // ok
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
						BitSize:  lo.ToPtr[int8](32),
						MinValue: lo.ToPtr[int64](-10),
						MaxValue: lo.ToPtr[uint64](98),
					},
				},
			},
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	baseSuite.OnEachRow(table, func(row []any) {
		require.Len(t, len(row), 1)
		number, ok := row[0].(int64)
		require.True(t, ok)
		require.True(t, number >= -10 && number <= 98)
	})
}
