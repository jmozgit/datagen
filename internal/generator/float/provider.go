package float

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/mo"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/model"
)

var ErrInvalidByteSize = errors.New("invalid byte size")

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	_ model.DatasetSchema,
	optUserSettings mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (model.AcceptanceDecision, error) {
	const fnName = "float accept"

	userSettings, uOk := optUserSettings.Get()
	if uOk && userSettings.Type != config.GeneratorTypeFloat {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	baseType, bOk := optBaseType.Get()
	if !uOk && bOk && baseType.Type != model.Float {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	fixedSize := int8(baseType.FixedSize)
	if uOk && userSettings.Float.ByteSize != nil {
		fixedSize = *userSettings.Float.ByteSize
	}

	reason := model.AcceptanceReasonColumnType
	if uOk {
		reason = model.AcceptanceUserSettings
	}

	switch fixedSize {
	case 4:
		return model.AcceptanceDecision{
			Generator:  newFloat32Gen(),
			AcceptedBy: reason,
		}, nil
	case 8:
		return model.AcceptanceDecision{
			Generator:  newFloat64Gen(),
			AcceptedBy: reason,
		}, nil
	default:
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %d %s", ErrInvalidByteSize, fixedSize, fnName)
	}
}
