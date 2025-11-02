package e2e_test

import (
	"testing"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/tests/suite"
	"github.com/stretchr/testify/require"
)

func Test_DriverArray(t *testing.T) {
	bs := suite.NewBaseSuite(t)
	table := bs.NewTable("array_driver_test", []suite.Column{
		suite.NewColumn("array_of_int", suite.TypeArrayInt),
		suite.NewColumn("array_of_string", suite.TypeArrayString),
	})
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(10),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitRows:  42,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 2)
		for _, v := range toSlice(t, row[0]) {
			_ = toInteger(t, v)
		}
		for _, v := range toSlice(t, row[1]) {
			_ = toString(t, v)
		}
		cnt++
	})
	require.GreaterOrEqual(t, cnt, 42)
}

func Test_UserSettingsArray(t *testing.T) {
	bs := suite.NewBaseSuite(t)
	table := bs.NewTable("array_user_settings_test", []suite.Column{
		suite.NewColumn("array_of_int", suite.TypeArrayInt),
		suite.NewColumn("array_of_string", suite.TypeArrayString),
		suite.NewColumn("array_of_lua", suite.TypeArrayString),
	})
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(7),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitBytes: 0,
			LimitRows:  27,
			Generators: []config.Generator{
				{
					Column: "array_of_int",
					Type:   config.GeneratorTypeArray,
				},
				{
					Column: "array_of_string",
					Type:   config.GeneratorTypeArray,
					Array: &config.Array{
						Rows: 1,
						Cols: 5,
						ElemType: &config.Generator{
							Type: config.GeneratorTypeText,
							Text: &config.Text{
								CharLenFrom: 5,
								CharLenTo:   5,
							},
						},
					},
				},
				{
					Column: "array_of_lua",
					Type:   config.GeneratorTypeArray,
					Array: &config.Array{
						Rows: 2,
						Cols: 3,
						ElemType: &config.Generator{
							Type: config.GeneratorTypeLua,
							Lua:  &config.Lua{Path: "./lua/random_string.lua"},
						},
					},
				},
			},
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 3)
		for _, v := range toSlice(t, row[0]) {
			_ = toInteger(t, v)
		}
		for _, v := range toSlice(t, row[1]) {
			str := toString(t, v)
			require.Len(t, str, 5)
		}
		row2Cnt := 0
		for _, v := range toSlice(t, row[2]) {
			_ = toString(t, v)
			row2Cnt++
		}
		require.Equal(t, row2Cnt, 6)
		cnt++
	})
	require.GreaterOrEqual(t, cnt, 27)
}

func toSlice(t *testing.T, val any) []any {
	t.Helper()

	switch v := val.(type) {
	case []any:
		return v
	default:
		require.Failf(t, "mismatched", "expected []any, not %T (%v)", val, val)
	}

	return nil
}
