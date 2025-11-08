package postgres_test

import (
	"os"
	"testing"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/testconn/options"
	testpg "github.com/jmozgit/datagen/internal/pkg/testconn/postgres"
	"github.com/jmozgit/datagen/internal/pkg/xrand"
	"github.com/jmozgit/datagen/internal/saver/postgres"

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
			Schema: model.PGIdentifier("public"),
			Table:  model.PGIdentifier("test"),
		},
		Columns: []model.Column{
			{Name: model.PGIdentifier("id"), Type: "integer", IsNullable: false, FixedSize: 4},
			{Name: model.PGIdentifier("comment"), Type: "text", IsNullable: false, FixedSize: -1},
		},
	})

	data := make([][]any, 23)
	for i := range data {
		data[i] = []any{i, xrand.LowerCaseString(10)}
	}

	schema := model.DatasetSchema{
		TableName: model.TableName{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("test")},
		Columns: []model.TargetType{
			//nolint:exhaustruct // ok for tests
			{
				SourceName: model.PGIdentifier("id"),
				SourceType: "integer",
			},
			//nolint:exhaustruct // ok for tests
			{
				SourceName: model.PGIdentifier("comment"),
				SourceType: "text",
			},
		},
	}

	batch := model.SaveBatch{
		SavingHints: setup.connect.PrepareHints(t.Context(), schema),
		Schema:      schema,
		Data:        data,
		Invalid:     make([]bool, len(data)),
	}

	saved, err := setup.connect.Save(
		t.Context(),
		batch,
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 0,
		RowsSaved:           len(data),
	}, saved.Stat)
	cntInvalid := 0
	for _, d := range batch.Invalid {
		if d {
			cntInvalid++
		}
	}
	require.Equal(t, 0, cntInvalid)
}

func Test_DbSaveManyDuplicates(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: model.PGIdentifier("public"),
			Table:  model.PGIdentifier("test_with_pk"),
		},
		Columns: []model.Column{
			{Name: model.PGIdentifier("id"), Type: "integer", IsNullable: false, FixedSize: 4},
		},
	}, options.WithPKs([]string{"id"}))

	data := make([][]any, 0, 26)
	for i := range cap(data) / 2 {
		data = append(data, []any{i}, []any{i})
	}

	schema := model.DatasetSchema{
		TableName: model.TableName{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("test_with_pk")},
		Columns: []model.TargetType{
			//nolint:exhaustruct // ok for tests
			{SourceName: model.PGIdentifier("id"), SourceType: "integer"},
		},
	}

	batch := model.SaveBatch{
		SavingHints: setup.connect.PrepareHints(t.Context(), schema),
		Schema:      schema,
		Data:        data,
		Invalid:     make([]bool, len(data)),
	}

	saved, err := setup.connect.Save(
		t.Context(), batch,
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 13,
		RowsSaved:           13,
	}, saved.Stat)
	cntInvalid := 0
	for _, d := range batch.Invalid {
		if d {
			cntInvalid++
		}
	}
	require.Equal(t, 13, cntInvalid)
}

func Test_OnlyOneUniqueRow(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: model.PGIdentifier("public"),
			Table:  model.PGIdentifier("test_with_pk"),
		},
		Columns: []model.Column{
			{Name: model.PGIdentifier("id"), Type: "integer", IsNullable: false, FixedSize: 4},
		},
	}, options.WithPKs([]string{"id"}))

	data := make([][]any, 0, 26)
	for range cap(data) {
		data = append(data, []any{10})
	}

	schema := model.DatasetSchema{
		TableName: model.TableName{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("test_with_pk")},
		Columns: []model.TargetType{
			//nolint:exhaustruct // ok for tests
			{
				SourceName: model.PGIdentifier("id"),
				SourceType: "integer",
			},
		},
	}
	batch := model.SaveBatch{
		SavingHints: setup.connect.PrepareHints(t.Context(), schema),
		Schema:      schema,
		Data:        data,
		Invalid:     make([]bool, len(data)),
	}

	saved, err := setup.connect.Save(
		t.Context(), batch,
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 25,
		RowsSaved:           1,
	}, saved.Stat)
	cntInvalid := 0
	for _, d := range batch.Invalid {
		if d {
			cntInvalid++
		}
	}
	require.Equal(t, 25, cntInvalid)
}

func Test_ColumnConstraint(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: model.PGIdentifier("public"),
			Table:  model.PGIdentifier("test_with_check"),
		},
		Columns: []model.Column{
			{Name: model.PGIdentifier("id"), Type: "integer CHECK (id > 10)", IsNullable: false, FixedSize: 4},
		},
	})

	data := make([][]any, 0, 20)
	for i := range cap(data) {
		data = append(data, []any{i})
	}

	schema := model.DatasetSchema{
		TableName: model.TableName{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("test_with_check")},
		Columns: []model.TargetType{
			//nolint:exhaustruct // it's okay here
			{
				SourceName: model.PGIdentifier("id"),
				SourceType: "integer",
			},
		},
	}

	batch := model.SaveBatch{
		SavingHints: setup.connect.PrepareHints(t.Context(), schema),
		Schema:      schema,
		Invalid:     make([]bool, len(data)),
		Data:        data,
	}

	saved, err := setup.connect.Save(
		t.Context(), batch,
	)
	require.NoError(t, err)
	require.Equal(t, model.SaveReport{
		ConstraintViolation: 11,
		RowsSaved:           9,
	}, saved.Stat)

	cntInvalid := 0
	for _, d := range batch.Invalid {
		if d {
			cntInvalid++
		}
	}
	require.Equal(t, 11, cntInvalid)
}

func Test_StringOfString(t *testing.T) {
	t.Parallel()

	setup := newSaveSetup(t, model.Table{
		Name: model.TableName{
			Schema: model.PGIdentifier("public"),
			Table:  model.PGIdentifier("test_with_pk"),
		},
		Columns: []model.Column{
			{Name: model.PGIdentifier("www"), Type: "text[]", IsNullable: false, FixedSize: 4},
		},
	})

	sliceOfSliceOfString := func() any {
		str := make([][]any, 4)
		for i := range str {
			str[i] = make([]any, 4)
		}
		for i := range str {
			for j := range str[i] {
				str[i][j] = xrand.LowerCaseString(10)
			}
		}
		return str
	}

	data := make([][]any, 10)
	for i := range cap(data) {
		data[i] = []any{sliceOfSliceOfString()}
	}

	schema := model.DatasetSchema{
		TableName: model.TableName{Schema: model.PGIdentifier("public"), Table: model.PGIdentifier("test_with_pk")},
		Columns: []model.TargetType{
			//nolint:exhaustruct // ok for tests
			{
				SourceName: model.PGIdentifier("www"),
				SourceType: "_text",
			},
		},
	}
	batch := model.SaveBatch{
		SavingHints: setup.connect.PrepareHints(t.Context(), schema),
		Schema:      schema,
		Data:        data,
		Invalid:     make([]bool, len(data)),
	}

	_, err := setup.connect.Save(
		t.Context(), batch,
	)
	require.NoError(t, err)
}
