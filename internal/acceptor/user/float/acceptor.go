package float

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator/float"
	"github.com/viktorkomarov/datagen/internal/model"
)

var (
	ErrUnsupportedByteSize = errors.New("byte size is unsupported")
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "user float: accept"

	userSettings, uOk := req.UserSettings.Get()
	if !uOk || userSettings.Type != config.GeneratorTypeFloat {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}
	floatSettings := userSettings.Float

	byteSize := int8(0)
	if floatSettings != nil && floatSettings.ByteSize != nil {
		byteSize = *floatSettings.ByteSize
	}

	if byteSize != 0 && req.BaseType.IsPresent() {
		baseType, _ := req.BaseType.Get()
		// try to use base type size
		byteSize = int8(baseType.FixedSize)
	}

	const (
		floatDefault = 0
		float32Size  = 4
		float64Size  = 8
	)

	switch byteSize {
	case floatDefault, float64Size:
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceUserSettings,
			Generator:  float.NewUnboundedFloat64Generator(),
		}, nil
	case float32Size:
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceUserSettings,
			Generator:  float.NewUnboundedFloat32Generator(),
		}, nil
	default:
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %d %s", ErrUnsupportedByteSize, byteSize, fnName)
	}
}
