package float

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/float"
	"github.com/viktorkomarov/datagen/internal/model"
)

var ErrFloatSizeUnspecified = errors.New("float size unspecified")

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "common type float: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Float {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	const (
		float32Gen = 4
		float64Gen = 8
	)

	switch baseType.FixedSize {
	case -1:
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	case 0, float32Gen:
		return model.AcceptanceDecision{
			AcceptedBy:     model.AcceptanceReasonColumnType,
			Generator:      float.NewUnboundedFloat32Generator(),
			ChooseCallback: nil,
		}, nil
	case float64Gen:
		return model.AcceptanceDecision{
			AcceptedBy:     model.AcceptanceReasonColumnType,
			Generator:      float.NewUnboundedFloat64Generator(),
			ChooseCallback: nil,
		}, nil
	default:
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", ErrFloatSizeUnspecified, fnName)
	}
}
