package plugin

import (
	"context"
	"errors"
	"fmt"
	"plugin"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/config"
	"github.com/jmozgit/datagen/internal/generator/fn"
	"github.com/jmozgit/datagen/internal/model"
)

var ErrInvalidPluginContract = errors.New("invalid plugin")

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "plugin: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypePlugin || userSettings.Plugin == nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	pluginPath := userSettings.Plugin.Path

	plg, err := plugin.Open(pluginPath)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	f, err := plg.Lookup("Gen")
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	genFn, ok := f.(func(context.Context) (any, error))
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", ErrInvalidPluginContract, fnName)
	}

	return model.AcceptanceDecision{
		ChooseCallback: nil,
		Generator:      fn.NewGenerator(genFn),
		AcceptedBy:     model.AcceptanceUserSettings,
	}, nil
}
