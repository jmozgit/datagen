package e2e_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/tests/suite"
)

func Test_LuaUserSettings(t *testing.T) {
	bs := suite.NewBaseSuite(t)

	table := bs.NewTable("lua_scripts", []suite.Column{
		suite.NewColumn("bool", suite.TypeBoolean),
		suite.NewColumn("string", suite.TypeText),
		suite.NewColumn("number", suite.TypeFloat8),
	})
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(16),
		suite.WithTableTarget(config.Table{
			Schema: table.Schema,
			Table:  table.Name,
			Generators: []config.Generator{
				{
					Column: "bool",
					Type:   config.GeneratorTypeLua,
					Lua:    &config.Lua{Path: "./lua/random_bool.lua"},
				},
				{
					Column: "string",
					Type:   config.GeneratorTypeLua,
					Lua:    &config.Lua{Path: "./lua/random_string.lua"},
				},
				{
					Column: "number",
					Type:   config.GeneratorTypeLua,
					Lua:    &config.Lua{Path: "./lua/random_number.lua"},
				},
			},
			LimitRows: 137,
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	bs.OnEachRow(table, func(row []any) {
		require.Len(t, row, 3)
		_ = toBoolean(t, row[0])
		_ = toString(t, row[1])
		_ = toFloat(t, row[2])
		cnt++
	})

	require.Equal(t, 137, cnt)
}

func toBoolean(t *testing.T, val any) bool {
	t.Helper()

	switch b := val.(type) {
	case bool:
		return b
	default:
		require.Failf(t, "boolean mismatched", "expected boolean type, not %T (%v)", val, val)
	}

	return false
}

func toString(t *testing.T, val any) string {
	t.Helper()

	switch b := val.(type) {
	case string:
		return b
	default:
		require.Failf(t, "striing mismatched", "expected string type, not %T (%v)", val, val)
	}

	return ""
}
