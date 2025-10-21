package text

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator/text"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "user text: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypeText {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	from, to := 20, 120
	if userSettings.Text != nil {
		from = userSettings.Text.CharLenFrom
		to = userSettings.Text.CharLenTo
	}

	if from > to {
		to = from + 20
	}

	var gen model.Generator
	if from == to {
		gen = text.NewFixedSizedStringGenerator(from)
	} else {
		gen = text.NewInRangeSizeGenerator(from, to)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceUserSettings,
		Generator:      gen,
		ChooseCallback: nil,
	}, nil
}
