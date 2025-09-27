package uuid

import (
	"context"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/xrand"

	gouuid "github.com/gofrs/uuid"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

var ErrUnknownVersion = errors.New("version is unknown")

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func isMatchUserSettings(optUserSettings mo.Option[config.Generator]) bool {
	userSetting, ok := optUserSettings.Get()

	return ok && userSetting.Type == config.GeneratorTypeUUID
}

func isMatchBaseType(optBaseType mo.Option[model.TargetType]) bool {
	baseType, ok := optBaseType.Get()

	return ok && baseType.Type == model.UUID
}

func (p Provider) Accept(
	_ context.Context,
	_ model.DatasetSchema,
	optUserSettings mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (model.AcceptanceDecision, error) {
	const fnName = "uuid: accept"

	userMatch := isMatchUserSettings(optUserSettings)
	baseTypeMatch := isMatchBaseType(optBaseType)

	match := userMatch || baseTypeMatch
	if !match {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	if userMatch {
		userSettings, _ := optUserSettings.Get()

		if userSettings.UUID == nil {
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator:  uuidV4Generator{},
			}, nil
		}

		version := lo.FromPtrOr(userSettings.UUID.Version, "v4")
		switch version {
		case "v1":
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator:  uuidV1Generator{},
			}, nil
		case "v3":
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator:  uuidV3Generator{},
			}, nil
		case "v4":
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator:  uuidV4Generator{},
			}, nil
		case "v5":
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator:  uuidV5Generator{},
			}, nil
		case "v6":
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator:  uuidV6Generator{},
			}, nil
		case "v7":
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator:  uuidV7Generator{},
			}, nil
		default:
			return model.AcceptanceDecision{}, fmt.Errorf("%w: %s %s", ErrUnknownVersion, version, fnName)
		}
	}

	return model.AcceptanceDecision{
		Generator:  uuidV4Generator{},
		AcceptedBy: model.AcceptanceReasonColumnType,
	}, nil
}

const systemNameLen = 10

type uuidV1Generator struct{}

func (u uuidV1Generator) Gen(_ context.Context) (any, error) {
	val, err := gouuid.NewV1()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v1 gen", err)
	}

	return val, nil
}

type uuidV3Generator struct{}

func (u uuidV3Generator) Gen(_ context.Context) (any, error) {
	v4, err := gouuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v3 gen", err)
	}

	return gouuid.NewV3(v4, xrand.LowerCaseString(systemNameLen)), nil
}

type uuidV4Generator struct{}

func (u uuidV4Generator) Gen(_ context.Context) (any, error) {
	val, err := gouuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v4 gen", err)
	}

	return val, nil
}

type uuidV5Generator struct{}

func (u uuidV5Generator) Gen(_ context.Context) (any, error) {
	v4, err := gouuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v3 gen", err)
	}

	return gouuid.NewV5(v4, xrand.LowerCaseString(systemNameLen)), nil
}

type uuidV6Generator struct{}

func (u uuidV6Generator) Gen(_ context.Context) (any, error) {
	val, err := gouuid.NewV6()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v6 gen", err)
	}

	return val, nil
}

type uuidV7Generator struct{}

func (u uuidV7Generator) Gen(_ context.Context) (any, error) {
	val, err := gouuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("%w: uuid v7 gen", err)
	}

	return val, nil
}
