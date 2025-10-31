package contract

import (
	"context"
	"errors"

	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/samber/mo"
)

type GeneratorProvider interface {
	Accept(
		ctx context.Context,
		req AcceptRequest,
	) (model.AcceptanceDecision, error)
}

var (
	ErrGeneratorDeclined          = errors.New("generator is declined")
	ErrNoAvailableGenerators      = errors.New("no available generators")
	ErrTooManyGeneratorsAvailable = errors.New("too many generator available")
)

type GeneratorRegistry interface {
	GetGenerator(context.Context, AcceptRequest) (model.Generator, error)
}

type AcceptRequest struct {
	Dataset      model.DatasetSchema
	UserSettings mo.Option[config.Generator]
	BaseType     mo.Option[model.TargetType]
}
