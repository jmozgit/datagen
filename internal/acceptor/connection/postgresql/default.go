package postgresql

import (
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/numeric"
	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/serail"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
)

func DefaultProviderGenerators(conn db.Connect) []contract.GeneratorProvider {
	return []contract.GeneratorProvider{
		numeric.NewProvider(conn),
		serail.NewProvider(conn),
	}
}
