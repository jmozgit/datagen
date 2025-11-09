package e2e

import (
	"encoding/json"
	"testing"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/pkg/testconn/options"
	"github.com/jmozgit/datagen/tests/suite"
	"github.com/stretchr/testify/require"
)

func Test_JSONPluginBased(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	bs := suite.NewBaseSuite(t)
	table := bs.NewTable(
		"json_test",
		[]suite.Column{
			suite.NewColumn("id", suite.TypeSerialInt4),
			suite.NewColumnRawType("json", "json"),
			suite.NewColumnRawType("jsonb", "jsonb"),
		},
	)
	bs.CreateTable(table, options.WithPreserve())

	bs.SaveConfig(
		suite.WithBatchSize(12),
		suite.WithTableTarget(config.Table{
			Schema:    table.Schema,
			Table:     table.Name,
			LimitRows: 39,
			Generators: []config.Generator{
				{
					Column: "json",
					Type:   config.GeneratorTypePlugin,
					Plugin: &config.Plugin{
						Path: "./plugins/json/json.so",
					},
				},
				{
					Column: "jsonb",
					Type:   config.GeneratorTypePlugin,
					Plugin: &config.Plugin{
						Path: "./plugins/json/json.so",
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
		_ = toRawJSON(t, row[1])
		_ = toRawJSON(t, row[2])
		cnt++
	})

	require.GreaterOrEqual(t, cnt, 39)
}

func toRawJSON(t *testing.T, val any) json.RawMessage {
	t.Helper()

	switch v := val.(type) {
	case json.RawMessage:
		return v
	case map[string]any:
		raw, err := json.Marshal(v)
		require.NoError(t, err)

		return json.RawMessage(raw)
	}
	require.Failf(t, "mismatch json", "unexpected json type %T", val)

	return nil
}
