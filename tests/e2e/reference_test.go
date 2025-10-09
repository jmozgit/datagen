package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/tests/suite"
)

func Test_ReferenceSimultaneousGeneration(t *testing.T) {
	bs := suite.NewBaseSuite(t)
	baseTable := suite.Table{
		Name: bs.TableName(suite.ScemaDefault, "base_table"),
		Columns: []suite.Column{
			suite.NewColumn("id", suite.TypeInt4),
		},
	}
	bs.CreateTable(baseTable, options.WithPKs([]string{"id"}), options.WithPreserve())

	childTable := suite.Table{
		Name: bs.TableName(suite.ScemaDefault, "child_table"),
		Columns: []suite.Column{
			suite.NewColumn("id", suite.TypeSerialInt4),
			suite.NewColumn("base_id", suite.TypeInt4),
		},
	}
	bs.CreateTable(
		childTable,
		options.WithPKs([]string{"id"}),
		options.WithForeignKey("foreign key (base_id) references base_table(id)"),
		options.WithPreserve(),
	)

	bs.SaveConfig(
		suite.WithBatchSize(10),
		suite.WithTableTarget(config.Table{
			Schema:     string(baseTable.Name.Schema),
			Table:      string(baseTable.Name.Table),
			LimitRows:  5,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
		suite.WithTableTarget(config.Table{
			Schema:     string(childTable.Name.Schema),
			Table:      string(childTable.Name.Table),
			LimitRows:  33,
			LimitBytes: 0,
			Generators: make([]config.Generator, 0),
		}),
	)

	err := bs.RunDatagen(t.Context(), suite.WithWorkers(1)) // disable parallel
	require.NoError(t, err)
}
