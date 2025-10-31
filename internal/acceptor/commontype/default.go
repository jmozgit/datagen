package commontype

import (
	"github.com/jmozgit/datagen/internal/acceptor/commontype/array"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/date"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/float"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/integer"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/text"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/time"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/uuid"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
)

func DefaultProviderGenerators(
	registry contract.GeneratorRegistry,
) []contract.GeneratorProvider {
	return []contract.GeneratorProvider{
		float.NewProvider(),
		integer.NewProvider(),
		time.NewProvider(),
		uuid.NewProvider(),
		date.NewProvider(),
		text.NewProvider(),
		array.NewProvider(registry),
	}
}
