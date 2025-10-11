package geometry

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/geometry"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

var geometryGenerator = map[string]struct{}{
	"box":     {},
	"circle":  {},
	"line":    {},
	"lseg":    {},
	"path":    {},
	"point":   {},
	"polygon": {},
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql geometry: accept"

	baseType, ok := req.BaseType.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	_, ok = geometryGenerator[baseType.SourceType]
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	gen, err := geometry.NewGenerator(baseType.SourceType)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
		Generator:      gen,
		ChooseCallback: nil,
	}, nil
}
