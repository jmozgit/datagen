package postgresql

import (
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/enum"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/numeric"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/serial"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
)

func DefaultProviderGenerators(conn db.Connect) []contract.GeneratorProvider {
	return []contract.GeneratorProvider{
		numeric.NewProvider(conn),
		serial.NewProvider(conn),
		enum.NewProvider(conn),
	}
}
