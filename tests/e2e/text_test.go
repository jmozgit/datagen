package e2e_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/tests/suite"
)

func Test_PostgresqlText(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)
	table := bs.NewTable("text_columns", []suite.Column{
		suite.NewColumnRawType("text", "text"),
		suite.NewColumnRawType("varchar_n", "varchar(120)"),
		suite.NewColumnRawType("char_n", "char(39)"),
		suite.NewColumnRawType("bpchar", "bpchar"),
	})
	bs.CreateTable(table, options.WithPreserve())

	bs.SaveConfig(
		suite.WithBatchSize(10),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitBytes: 0,
			LimitRows:  143,
			Generators: make([]config.Generator, 0),
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	constraints := []int{0, 120, 39, 0}
	cnt := 0
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 4)
		for i := 0; i < len(row); i++ {
			str := toString(t, row[i])
			if constraints[i] != 0 {
				require.Len(t, str, constraints[i])
			}
		}
		cnt++
	})

	require.GreaterOrEqual(t, cnt, 143)
}

func Test_UserSettingsText(t *testing.T) {
	bs := suite.NewBaseSuite(t)
	table := bs.NewTable("user_settings_columns", []suite.Column{
		suite.NewColumn("unlimited_text", suite.TypeText),
		suite.NewColumn("fixed_text", suite.TypeText),
		suite.NewColumn("range_text", suite.TypeText),
	})
	bs.CreateTable(table, options.WithPreserve())

	bs.SaveConfig(
		suite.WithBatchSize(59),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitBytes: 0,
			LimitRows:  67,
			Generators: []config.Generator{
				{Column: "unlimited_text", Type: config.GeneratorTypeText},
				{Column: "fixed_text", Type: config.GeneratorTypeText, Text: &config.Text{CharLenFrom: 10, CharLenTo: 10}},
				{Column: "range_text", Type: config.GeneratorTypeText, Text: &config.Text{CharLenFrom: 10, CharLenTo: 40}},
			},
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 3)
		_ = toString(t, row[0])
		r1 := toString(t, row[1])
		require.Len(t, r1, 10)
		r2 := toString(t, row[2])
		require.True(t, 10 <= len(r2) && len(r2) <= 40)
		cnt++
	})

	require.GreaterOrEqual(t, cnt, 67)
}
