package contract

import (
	"context"
	"errors"

	"github.com/samber/mo"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/model"
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

type AcceptRequest struct {
	Dataset      model.DatasetSchema
	UserSettings mo.Option[config.Generator]
	BaseType     mo.Option[model.TargetType]
}
