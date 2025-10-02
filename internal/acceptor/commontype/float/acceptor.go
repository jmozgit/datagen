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
	if !ok && baseType.Type != model.Float {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	const (
		float32Gen = 32
		float64Gen = 64
	)

	switch baseType.FixedSize {
	case 0, float32Gen:
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceReasonColumnType,
			Generator:  float.NewUnboundedFloat32Generator(),
		}, nil
	case float64Gen:
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceReasonColumnType,
			Generator:  float.NewUnboundedFloat64Generator(),
		}, nil
	default:
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", ErrFloatSizeUnspecified, fnName)
	}
}
