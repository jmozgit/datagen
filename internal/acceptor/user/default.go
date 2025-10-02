package user

import (
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/acceptor/user/float"
	"github.com/viktorkomarov/datagen/internal/acceptor/user/integer"
	"github.com/viktorkomarov/datagen/internal/acceptor/user/lua"
	"github.com/viktorkomarov/datagen/internal/acceptor/user/time"
	"github.com/viktorkomarov/datagen/internal/acceptor/user/uuid"
)

func DefaultProviderGenerators() []contract.GeneratorProvider {
	return []contract.GeneratorProvider{
		integer.NewProvider(),
		float.NewProvider(),
		time.NewProvider(),
		uuid.NewProvider(),
		lua.NewProvider(),
	}
}
