package postgresql

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/bytea"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/enum"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/geometry"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/interval"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/network"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/numeric"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/oid"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/reference"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/reuse"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/serial"
	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/text"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/pkg/db/adapter/pgx"
	"github.com/jmozgit/datagen/internal/refresolver"
)

func DefaultProviderGenerators(
	pool *pgxpool.Pool,
	refResolver *refresolver.Service,
	setter contract.SetterOptionBasedGenerator,
) ([]contract.GeneratorProvider, error) {
	conn := pgx.NewAdapterPool(pool)

	if err := setter.SetReuseValuesGeneratorProvider(reuse.NewProvider(conn)); err != nil {
		return nil, fmt.Errorf("%w: postgresql default provider generator", err)
	}

	return []contract.GeneratorProvider{
		numeric.NewProvider(conn),
		serial.NewProvider(conn),
		enum.NewProvider(conn),
		interval.NewProvider(),
		reference.NewProvider(conn, refResolver),
		geometry.NewProvider(),
		network.NewProvider(),
		text.NewProvider(conn),
		oid.NewProvider(pool, refResolver),
		bytea.NewProvider(),
	}, nil
}
