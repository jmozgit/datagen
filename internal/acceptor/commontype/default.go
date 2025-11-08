package commontype

import (
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/commontype/array"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/date"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/float"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/integer"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/null"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/text"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/time"
	"github.com/jmozgit/datagen/internal/acceptor/commontype/uuid"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
)

func DefaultProviderGenerators(
	registry contract.GeneratorRegistry,
	setter contract.SetterOptionBasedGenerator,
) ([]contract.GeneratorProvider, error) {
	gens := []contract.GeneratorProvider{
		float.NewProvider(),
		integer.NewProvider(),
		time.NewProvider(),
		uuid.NewProvider(),
		date.NewProvider(),
		text.NewProvider(),
		array.NewProvider(registry),
	}

	if err := setter.SetWithNullValuesGeneratorProvider(null.NewProvider()); err != nil {
		return nil, fmt.Errorf("%w: default provider generators", err)
	}

	return gens, nil
}
