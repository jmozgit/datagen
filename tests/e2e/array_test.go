package e2e_test

import (
	"testing"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/pkg/testconn/options"
	"github.com/jmozgit/datagen/tests/suite"
	"github.com/stretchr/testify/require"
)

func Test_DriverArray(t *testing.T) {
	bs := suite.NewBaseSuite(t)
	table := bs.NewTable("array_driver_test", []suite.Column{
		suite.NewColumn("array_of_int", suite.TypeArrayInt),
		suite.NewColumn("array_of_string", suite.TypeArrayString),
	})
	bs.CreateTable(table, options.WithPreserve())

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

func toSlice(t *testing.T, val any) []any {
	t.Helper()

	switch v := val.(type) {
	case []any:
		return v
	default:
		require.Failf(t, "mismatched", "expected []any, not %T", val)
	}

	return nil
}
