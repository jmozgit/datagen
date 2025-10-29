package e2e_test

import (
	"testing"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/tests/suite"
	"github.com/stretchr/testify/require"
)

func Test_PostgresqlGeometry(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)

	table := bs.NewTable("geometry_table", []suite.Column{
		suite.NewColumnRawType("box", "box"),
		suite.NewColumnRawType("circle", "circle"),
		suite.NewColumnRawType("line", "line"),
		suite.NewColumnRawType("lseg", "lseg"),
		suite.NewColumnRawType("path", "path"),
		suite.NewColumnRawType("point", "point"),
		suite.NewColumnRawType("polygon", "polygon"),
	})
	bs.CreateTable(table)

	bs.SaveConfig(
		suite.WithBatchSize(15),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitRows:  100,
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
	require.GreaterOrEqual(t, cnt, 100)
}
