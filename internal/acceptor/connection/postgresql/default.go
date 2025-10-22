package postgresql

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/enum"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/geometry"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/interval"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/network"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/numeric"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/oid"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/reference"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/serial"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/text"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/pkg/db/adapter/pgx"
	"github.com/viktorkomarov/datagen/internal/refresolver"
)

func DefaultProviderGenerators(
	pool *pgxpool.Pool,
	refResolver *refresolver.Service,
) []contract.GeneratorProvider {
	conn := pgx.NewAdapterPool(pool)

	return []contract.GeneratorProvider{
		numeric.NewProvider(conn),
		serial.NewProvider(conn),
		enum.NewProvider(conn),
		interval.NewProvider(),
		reference.NewProvider(conn, refResolver),
		geometry.NewProvider(),
		network.NewProvider(),
		text.NewProvider(conn),
		oid.NewProvider(pool),
	}
}
