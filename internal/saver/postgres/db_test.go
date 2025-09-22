package postgres_test

import (
	"os"
	"testing"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/testconn/options"
	testpg "github.com/viktorkomarov/datagen/internal/pkg/testconn/postgres"
	"github.com/viktorkomarov/datagen/internal/pkg/xrand"
	"github.com/viktorkomarov/datagen/internal/saver/postgres"

	"github.com/stretchr/testify/require"
)

type saveSetup struct {
	testConn *testpg.Conn
	connect  *postgres.DB
}

func newSaveSetup(
	t *testing.T,
	table model.Table,
	opts ...options.CreateTableOption,
) *saveSetup {
	t.Helper()

	connStr := os.Getenv("TEST_DATAGEN_PG_CONN")
	if connStr == "" {
		t.Skipf("test pg env host isn't set")
	}

	tmpConn, err := testpg.New(t, connStr)
	require.NoError(t, err)

	err = tmpConn.CreateTable(t.Context(), table, opts...)
	require.NoError(t, err)

	sqlConn := config.SQLConnection{
		Host:     tmpConn.Raw().Config().Host,
		Port:     int(tmpConn.Raw().Config().Port),
		User:     tmpConn.Raw().Config().User,
		Password: tmpConn.Raw().Config().Password,
		DBName:   tmpConn.Raw().Config().Database,
		Options:  make([]string, 0),
	}

	connect, err := postgres.New(t.Context(), sqlConn.ConnString("postgresql"))
	require.NoError(t, err)

	return &saveSetup{
		testConn: tmpConn,
		connect:  connect,
	}
}

func Test_DbSaveNoErrors(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test",
		},
		Columns: []model.Column{
			{Name: "id", Type: "integer", IsNullable: false, FixedSize: 4, IsSerial: false, ColumnDefault: ""},
			{Name: "comment", Type: "text", IsNullable: false, FixedSize: -2, IsSerial: false, ColumnDefault: ""},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	})

	data := make([][]any, 23)
	for i := range data {
		data[i] = []any{i, xrand.LowerCaseString(10)}
	}

	saved, err := setup.connect.Save(
		t.Context(),
		model.SaveBatch{
			Schema: model.DatasetSchema{
				ID: "public.test",
				DataTypes: []model.TargetType{
					//nolint:exhaustruct // ok for tests
					{
						SourceName: "id",
						SourceType: "integer",
					},
					//nolint:exhaustruct // ok for tests
					{
						SourceName: "comment",
						SourceType: "text",
					},
				},
				UniqueConstraints: make([]model.UniqueConstraints, 0),
			},
			ExcludeTargets: make(map[model.Identifier]struct{}),
			Data:           data,
		},
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 0,
		RowsSaved:           len(data),
		BytesSaved:          0,
	}, saved)
}

func Test_DbSaveManyDuplicates(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_with_pk",
		},
		Columns: []model.Column{
			{Name: "id", Type: "integer", IsNullable: false, FixedSize: 4, IsSerial: false, ColumnDefault: ""},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	}, options.WithPKs([]string{"id"}))

	data := make([][]any, 0, 26)
	for i := range cap(data) / 2 {
		data = append(data, []any{i}, []any{i})
	}

	saved, err := setup.connect.Save(
		t.Context(),
		model.SaveBatch{
			Schema: model.DatasetSchema{
				ID: "public.test_with_pk",
				DataTypes: []model.TargetType{
					//nolint:exhaustruct // ok for tests
					{SourceName: "id", SourceType: "integer"},
				},
				UniqueConstraints: make([]model.UniqueConstraints, 0),
			},
			Data:           data,
			ExcludeTargets: make(map[model.Identifier]struct{}),
		},
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 13,
		RowsSaved:           13,
		BytesSaved:          0,
	}, saved)
}

func Test_OnlyOneUniqueRow(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_with_pk",
		},
		Columns: []model.Column{
			{Name: "id", Type: "integer", IsNullable: false, FixedSize: 4, IsSerial: false, ColumnDefault: ""},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	}, options.WithPKs([]string{"id"}))

	data := make([][]any, 0, 26)
	for range cap(data) {
		data = append(data, []any{10})
	}

	saved, err := setup.connect.Save(
		t.Context(),
		model.SaveBatch{
			Schema: model.DatasetSchema{
				ID: "public.test_with_pk",
				DataTypes: []model.TargetType{
					//nolint:exhaustruct // ok for tests
					{
						SourceName: "id",
						SourceType: "integer",
					},
				},
				UniqueConstraints: make([]model.UniqueConstraints, 0),
			},
			ExcludeTargets: make(map[model.Identifier]struct{}),
			Data:           data,
		},
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 25,
		RowsSaved:           1,
		BytesSaved:          0,
	}, saved)
}

func Test_ColumnConstraint(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_with_check",
		},
		Columns: []model.Column{
			{Name: "id", Type: "integer CHECK (id > 10)", IsNullable: false, FixedSize: 4, IsSerial: false, ColumnDefault: ""},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	})

	data := make([][]any, 0, 20)
	for i := range cap(data) {
		data = append(data, []any{i})
	}

	saved, err := setup.connect.Save(
		t.Context(),
		model.SaveBatch{
			Schema: model.DatasetSchema{
				ID: "public.test_with_check",
				DataTypes: []model.TargetType{
					//nolint:exhaustruct // it's okay here
					{
						SourceName: "id",
						SourceType: "integer",
					},
				},
				UniqueConstraints: make([]model.UniqueConstraints, 0),
			},
			ExcludeTargets: make(map[model.Identifier]struct{}),
			Data:           data,
		},
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 11,
		RowsSaved:           9,
		BytesSaved:          0,
	}, saved)
}

func Test_SaveAllDefaults(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: "public",
			Table:  "test_all_defaults",
		},
		Columns: []model.Column{
			{Name: "serial", Type: "serial", IsNullable: false, FixedSize: 4, IsSerial: false, ColumnDefault: ""},
			{Name: "smallserial", Type: "smallserial", IsNullable: false, FixedSize: 4, IsSerial: false, ColumnDefault: ""},
			{Name: "bigserial", Type: "bigserial", IsNullable: false, FixedSize: 4, IsSerial: false, ColumnDefault: ""},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	})

	_, err := setup.connect.SaveAllDefaultValues(t.Context(), model.DatasetSchema{
		ID: "public.test_all_defaults",
		DataTypes: []model.TargetType{
			//nolint:exhaustruct // it's okay
			{SourceName: "serial"},
			//nolint:exhaustruct // it's okay
			{SourceName: "smallserial"},
			//nolint:exhaustruct // it's okay
			{SourceName: "bigserial"},
		},
		UniqueConstraints: make([]model.UniqueConstraints, 0),
	}, 100)
	require.NoError(t, err)

	cnt := 0
	err = setup.testConn.Raw().QueryRow(t.Context(), "SELECT COUNT(*) FROM public.test_all_defaults").Scan(&cnt)
	require.NoError(t, err)
	require.Equal(t, 100, cnt)
}
