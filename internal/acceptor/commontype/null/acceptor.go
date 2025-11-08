package null

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/null"
	"github.com/jmozgit/datagen/internal/model"
)

type Provider struct {
}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "null: accept"

	baseGen, ok := req.BaseGenerator.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	settings, ok := req.UserSettings.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	return model.AcceptanceDecision{
		Generator:      null.NewGenerator(settings.NullFraction, baseGen),
		ChooseCallback: nil,
		AcceptedBy:     model.AcceptanceReasonColumnType,
	}, nil
}
