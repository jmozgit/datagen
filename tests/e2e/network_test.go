package e2e_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/tests/suite"
)

func Test_PostgresqlNetwork(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)

	table := bs.NewTable("network_table", []suite.Column{
		suite.NewColumnRawType("inet", "inet"),
		suite.NewColumnRawType("cidr", "cidr"),
		suite.NewColumnRawType("macaddr", "macaddr"),
		suite.NewColumnRawType("macaddr8", "macaddr8"),
	})
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(17),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitRows:  123,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
	)

	err := bs.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	bs.OnEachRow(table, func(_ []any) {
		cnt++
	})
	require.GreaterOrEqual(t, cnt, 123)
}
