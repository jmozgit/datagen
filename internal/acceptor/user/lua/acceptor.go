package lua

import (
	"context"
	"fmt"
	"os"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator/lua"
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
	const fnName = "user lua: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypeLua {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	path := ""
	if userSettings.Lua != nil {
		path = userSettings.Lua.Path
	}

	_, err := os.Stat(path)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: filename %s %s", err, path, fnName)
	}

	return model.AcceptanceDecision{
		Generator:  lua.NewScriptExecutor(path),
		AcceptedBy: model.AcceptanceUserSettings,
	}, nil
}
