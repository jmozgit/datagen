package postgresql

import (
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/enum"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/geometry"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/interval"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/network"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/numeric"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/reference"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/serial"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/text"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
	"github.com/viktorkomarov/datagen/internal/refresolver"
)

func DefaultProviderGenerators(
	conn db.Connect,
	refResolver *refresolver.Service,
) []contract.GeneratorProvider {
	return []contract.GeneratorProvider{
		numeric.NewProvider(conn),
		serial.NewProvider(conn),
		enum.NewProvider(conn),
		interval.NewProvider(),
		reference.NewProvider(conn, refResolver),
		geometry.NewProvider(),
		network.NewProvider(),
		text.NewProvider(conn),
	}
}
