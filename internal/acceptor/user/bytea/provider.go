package bytea

import (
	"context"
	"fmt"

	"github.com/c2h5oh/datasize"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/generator/bytea"
	"github.com/jmozgit/datagen/internal/model"
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "user bytea: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypeBytea {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	dfltSize := datasize.KB * 100
	diff := datasize.KB * 20

	if userSettings.Bytea != nil {
		dfltSize = userSettings.Bytea.Size
		diff = userSettings.Bytea.Range
	}

	return model.AcceptanceDecision{
		ChooseCallback: nil,
		AcceptedBy:     model.AcceptanceUserSettings,
		Generator:      bytea.NewAroundByteaGenerator(dfltSize, diff),
	}, nil
}
