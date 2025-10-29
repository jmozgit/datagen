package probability

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/generator/probability"
	"github.com/jmozgit/datagen/internal/model"
)

var ErrEmptyProbabilityList = errors.New("empty probability list")

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "probability: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypeProbabilityList {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	if userSettings.ListProbability == nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", ErrEmptyProbabilityList, fnName)
	}

	ls := userSettings.ListProbability

	generator, err := probability.NewList(ls.Distribution, ls.Values)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		ChooseCallback: nil,
		AcceptedBy:     model.AcceptanceUserSettings,
		Generator:      generator,
	}, nil
}
