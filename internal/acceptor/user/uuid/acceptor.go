package uuid

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator/uuid"
	"github.com/viktorkomarov/datagen/internal/model"
)

var ErrUnknownVersion = errors.New("version is unknown")

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

var generatorsByVersion = map[string]model.Generator{
	"v1": uuid.NewUUIDV1Generator(),
	"v3": uuid.NewUUIDV3Generator(),
	"v4": uuid.NewUUIDV4Generator(),
	"v5": uuid.NewUUIDV5Generator(),
	"v6": uuid.NewUUIDV6Generator(),
	"v7": uuid.NewUUIDV7Generator(),
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "user uuid: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypeUUID {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}
	uuidSettings := userSettings.UUID

	version := "v4"
	if uuidSettings != nil && uuidSettings.Version != nil {
		version = *uuidSettings.Version
	}

	gen, ok := generatorsByVersion[version]
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s %s", ErrUnknownVersion, version, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceUserSettings,
		Generator:      gen,
		ChooseCallback: nil,
	}, nil
}
