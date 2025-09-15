package postgres

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	testpg "github.com/viktorkomarov/datagen/internal/pkg/testconn/postgres"
	"github.com/viktorkomarov/datagen/internal/pkg/xrand"
)

type saveSetup struct {
	testConn *testpg.Conn
	connect  *DB
}

func newSaveSetup(
	t *testing.T,
	table model.Table,
	opts ...testpg.CreateTableOption,
) *saveSetup {
	connStr := os.Getenv("TEST_DATAGEN_PG_CONN")
	if connStr == "" {
		t.Skipf("test pg env host isn't set")
	}

	tmpConn, err := testpg.New(t, connStr)
	require.NoError(t, err)

	err = tmpConn.CreateTable(t.Context(), table, opts...)
	require.NoError(t, err)

	// todo::refactor it
	sqlConn := config.SQLConnection{
		Host:     tmpConn.Raw().Config().Host,
		Port:     int(tmpConn.Raw().Config().Port),
		User:     tmpConn.Raw().Config().User,
		Password: tmpConn.Raw().Config().Password,
		DBName:   tmpConn.Raw().Config().Database,
	}

	connect, err := New(t.Context(), sqlConn.ConnString("postgresql"))
	require.NoError(t, err)

	return &saveSetup{
		testConn: tmpConn,
		connect:  connect,
	}
}

func Test_DbSaveNoErrors(t *testing.T) {
	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test",
		},
		Columns: []model.Column{
			{Name: "id", Type: "integer", IsNullable: false},
			{Name: "comment", Type: "text", IsNullable: false},
		},
	})

	data := make([][]any, 23)
	for i := range data {
		data[i] = []any{i, xrand.LowerCaseString(10)}
	}

	saved, err := setup.connect.Save(
		t.Context(),
		model.DatasetSchema{
			ID: "public.test",
			DataTypes: []model.TargetType{
				{
					SourceName: "id",
					SourceType: "integer",
				},
				{
					SourceName: "comment",
					SourceType: "text",
				},
			},
		},
		data,
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 0,
		RowsSaved:           len(data),
		BytesSaved:          0,
	}, saved)
}

func Test_DbSaveManyDuplicates(t *testing.T) {
	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_with_pk",
		},
		Columns: []model.Column{
			{Name: "id", Type: "integer", IsNullable: false},
		},
	}, testpg.WithPKs([]string{"id"}))

	data := make([][]any, 0, 26)
	for i := 0; i < cap(data)/2; i++ {
		data = append(data, []any{i})
		data = append(data, []any{i})
	}

	saved, err := setup.connect.Save(
		t.Context(),
		model.DatasetSchema{
			ID: "public.test_with_pk",
			DataTypes: []model.TargetType{
				{
					SourceName: "id",
					SourceType: "integer",
				},
			},
		},
		data,
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 13,
		RowsSaved:           13,
		BytesSaved:          0,
	}, saved)
}
