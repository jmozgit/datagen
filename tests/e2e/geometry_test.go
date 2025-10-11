package e2e_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/tests/suite"
)

func Test_PostgresqlGeometry(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)

	table := suite.Table{
		Name: bs.TableName(suite.ScemaDefault, "geometry_table"),
		Columns: []suite.Column{
			suite.NewColumnRawType("box", "box"),
			suite.NewColumnRawType("circle", "circle"),
			suite.NewColumnRawType("line", "line"),
			suite.NewColumnRawType("lseg", "lseg"),
			suite.NewColumnRawType("path", "path"),
			suite.NewColumnRawType("point", "point"),
			suite.NewColumnRawType("polygon", "polygon"),
		},
	}
	bs.CreateTable(table, options.WithPreserve())

	bs.SaveConfig(
		suite.WithBatchSize(15),
		suite.WithTableTarget(config.Table{
			Schema:     string(table.Name.Schema),
			Table:      string(table.Name.Table),
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
	require.Equal(t, cnt, 100)
}
