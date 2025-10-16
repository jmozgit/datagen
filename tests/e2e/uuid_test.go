package e2e_test

import (
	"testing"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	"github.com/viktorkomarov/datagen/tests/suite"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_UUID(t *testing.T) {
	suite.TestOnlyFor(t, "postgresql")

	baseSuite := suite.NewBaseSuite(t)
	table := baseSuite.NewTable("test_uuids",
		[]suite.Column{
			suite.NewColumnRawType("v1", "text"),
			suite.NewColumnRawType("v3", "text"),
			suite.NewColumnRawType("v4", "text"),
			suite.NewColumnRawType("v5", "text"),
			suite.NewColumnRawType("v6", "text"),
			suite.NewColumnRawType("v7", "text"),
			suite.NewColumnRawType("dflt", "uuid"),
		})

	generators := []config.Generator{
		//nolint:exhaustruct // oneof
		{Type: "uuid", Column: table.Columns[0].Name, UUID: &config.UUID{Version: lo.ToPtr("v1")}},
		//nolint:exhaustruct // oneof
		{Type: "uuid", Column: table.Columns[1].Name, UUID: &config.UUID{Version: lo.ToPtr("v3")}},
		//nolint:exhaustruct // oneof
		{Type: "uuid", Column: table.Columns[2].Name, UUID: &config.UUID{Version: lo.ToPtr("v4")}},
		//nolint:exhaustruct // oneof
		{Type: "uuid", Column: table.Columns[3].Name, UUID: &config.UUID{Version: lo.ToPtr("v5")}},
		//nolint:exhaustruct // oneof
		{Type: "uuid", Column: table.Columns[4].Name, UUID: &config.UUID{Version: lo.ToPtr("v6")}},
		//nolint:exhaustruct // oneof
		{Type: "uuid", Column: table.Columns[5].Name, UUID: &config.UUID{Version: lo.ToPtr("v7")}},
		//nolint:exhaustruct // oneof
		{Type: "uuid", Column: table.Columns[6].Name, UUID: &config.UUID{Version: nil}},
	}

	versions := []byte{1, 3, 4, 5, 6, 7, 4}

	baseSuite.CreateTable(table, options.WithPreserve())
	baseSuite.SaveConfig(
		suite.WithBatchSize(3),
		suite.WithTableTarget(config.Table{
			Schema:     table.Schema,
			Table:      table.Name,
			LimitRows:  9,
			LimitBytes: 0,
			Generators: generators,
		}),
	)

	err := baseSuite.RunDatagen(t.Context())
	require.NoError(t, err)

	cnt := 0
	baseSuite.OnEachRow(table, func(row []any) {
		require.Len(t, row, len(table.Columns))

		for i, val := range row {
			uuidVal := toUUIDString(t, val)

			id, err := uuid.FromString(uuidVal)
			require.NoError(t, err)

			assert.Equal(t, versions[i], id.Version())
		}
		cnt++
	})

	require.Equal(t, 9, cnt)
}

func toUUIDString(t *testing.T, val any) string {
	t.Helper()

	switch v := val.(type) {
	case string:
		return v
	case [16]uint8:
		val, err := uuid.FromBytes(v[:])
		require.NoError(t, err)

		return val.String()
	case pgtype.UUID:
		return v.String()
	default:
		require.Failf(t, "uuid mismatched", "expected uuid type, not %T (%v)", val, val)
	}

	return ""
}
