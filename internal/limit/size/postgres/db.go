package postgres

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/db"
)

type connector struct {
	oid     uint32
	connect db.Connect
}

func newConnector(ctx context.Context, connect db.Connect, table model.TableName) (*connector, error) {
	const query = `
	SELECT c.oid
		FROM pg_class c
	JOIN 
		information_schema.tables t ON t.table_name = c.relname AND t.table_schema = $1
	WHERE 
		t.table_name = $2
	`

	var oid uint32
	if err := connect.QueryRow(ctx, query, table.Schema.AsArgument(), table.Table.AsArgument()).Scan(&oid); err != nil {
		return nil, fmt.Errorf("%w: new table sizer", err)
	}

	return &connector{connect: connect, oid: oid}, nil
}

func (t *connector) TableSize(ctx context.Context) (uint64, error) {
	const query = `select pg_table_size($1)`

	var size uint64
	if err := t.connect.QueryRow(ctx, query, t.oid).Scan(&size); err != nil {
		return 0, fmt.Errorf("%w: table size", err)
	}

	return size, nil
}
