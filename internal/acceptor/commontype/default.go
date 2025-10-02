package commontype

import (
	"github.com/viktorkomarov/datagen/internal/acceptor/commontype/float"
	"github.com/viktorkomarov/datagen/internal/acceptor/commontype/integer"
	"github.com/viktorkomarov/datagen/internal/acceptor/commontype/time"
	"github.com/viktorkomarov/datagen/internal/acceptor/commontype/uuid"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
)

func DefaultProviderGenerators() []contract.GeneratorProvider {
	return []contract.GeneratorProvider{
		float.NewProvider(),
		integer.NewProvider(),
		time.NewProvider(),
		uuid.NewProvider(),
	}
}
