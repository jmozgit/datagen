package registry

import (
	"github.com/viktorkomarov/datagen/internal/generator/integer"
	"github.com/viktorkomarov/datagen/internal/model"
)

func defaultGeneratorProviders() []model.GeneratorProvider {
	return []model.GeneratorProvider{
		integer.New(),
	}
}
