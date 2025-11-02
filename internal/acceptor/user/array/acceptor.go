package array

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/generator/array"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/samber/mo"
)

type Provider struct {
	registry contract.GeneratorRegistry
}

func NewProvider(registry contract.GeneratorRegistry) *Provider {
	return &Provider{
		registry: registry,
	}
}

func (p *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "user array: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypeArray {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}
	arraySettings := userSettings.Array

	elemTypeSettings := mo.None[config.Generator]()
	if arraySettings != nil && arraySettings.ElemType != nil {
		elemTypeSettings = mo.Some(*arraySettings.ElemType)
	}
	req.UserSettings = elemTypeSettings

	if req.BaseType.IsPresent() {
		bt := req.BaseType.MustGet()
		bt.FixedSize = int(bt.ArrayElem.ElemSize)
		bt.SourceType = bt.ArrayElem.SourceType
		bt.Type = bt.ArrayElem.ElemType

		req.BaseType = mo.Some(bt)
	}

	rows, cols := 1, 3
	if arraySettings != nil && arraySettings.Rows != 0 {
		rows = int(arraySettings.Rows)
	}
	if arraySettings != nil && arraySettings.Cols != 0 {
		cols = int(arraySettings.Cols)
	}

	gen, err := p.registry.GetGenerator(ctx, req)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceUserSettings,
		Generator:      array.NewGenerator(rows, cols, gen),
		ChooseCallback: nil,
	}, nil
}
