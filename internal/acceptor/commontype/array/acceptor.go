package array

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/array"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/samber/mo"
)

type Provider struct {
	elemGens contract.GeneratorRegistry
}

func NewProvider(elemGens contract.GeneratorRegistry) *Provider {
	return &Provider{elemGens: elemGens}
}

func changeTypes(baseType model.TargetType, array model.ArrayInfo) model.TargetType {
	baseType.SourceType = array.SourceType
	baseType.Type = array.ElemType

	return baseType
}

func (p *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "common type array: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Array {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}
	req.BaseType = mo.Some(changeTypes(baseType, baseType.ArrayElem))

	gen, err := p.elemGens.GetGenerator(ctx, req)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		Generator: array.NewGenerator(
			1, 1, gen,
		),
		AcceptedBy:     model.AcceptanceReasonColumnType,
		ChooseCallback: nil,
	}, nil
}
