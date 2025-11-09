package user

import (
	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/acceptor/user/array"
	"github.com/jmozgit/datagen/internal/acceptor/user/bytea"
	"github.com/jmozgit/datagen/internal/acceptor/user/float"
	"github.com/jmozgit/datagen/internal/acceptor/user/integer"
	"github.com/jmozgit/datagen/internal/acceptor/user/lua"
	"github.com/jmozgit/datagen/internal/acceptor/user/plugin"
	"github.com/jmozgit/datagen/internal/acceptor/user/text"
	"github.com/jmozgit/datagen/internal/acceptor/user/time"
	"github.com/jmozgit/datagen/internal/acceptor/user/uuid"
)

func DefaultProviderGenerators(elemsGens contract.GeneratorRegistry) []contract.GeneratorProvider {
	return []contract.GeneratorProvider{
		integer.NewProvider(),
		float.NewProvider(),
		time.NewProvider(),
		uuid.NewProvider(),
		lua.NewProvider(),
		text.NewProvider(),
		bytea.NewProvider(),
		array.NewProvider(elemsGens),
		plugin.NewProvider(),
	}
}
