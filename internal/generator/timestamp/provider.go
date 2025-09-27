package timestamp

import (
	"context"
	"fmt"
	"time"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/samber/lo"
	"github.com/samber/mo"
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func isMatchUserSettings(optUserSettings mo.Option[config.Generator]) bool {
	userSettings, ok := optUserSettings.Get()

	return ok && userSettings.Type == config.GeneratorTypeTimestamp
}

func isMatchColumnType(optBaseType mo.Option[model.TargetType]) bool {
	baseType, ok := optBaseType.Get()

	return ok && baseType.Type == model.Timestamp
}

func (p Provider) Accept(
	_ context.Context,
	_ model.DatasetSchema,
	optUserSettings mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (model.AcceptanceDecision, error) {
	const fnName = "timestamp: accept"

	userSettingsMatch := isMatchUserSettings(optUserSettings)
	baseTypeMatch := isMatchColumnType(optBaseType)

	match := userSettingsMatch || baseTypeMatch
	if !match {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	if userSettingsMatch {
		userSettings, _ := optUserSettings.Get()

		if userSettings.Timestamp == nil || userSettings.Timestamp.OnlyNow {
			return model.AcceptanceDecision{
				AcceptedBy: model.AcceptanceUserSettings,
				Generator:  newAlwaysNow(),
			}, nil
		}

		const day720 = time.Hour * 24 * 720
		from := lo.FromPtrOr(userSettings.Timestamp.From, time.Now().Add(-day720))

		const day60 = time.Hour * 24 * 60
		to := lo.FromPtrOr(userSettings.Timestamp.To, time.Now().Add(day60))

		if to.Before(from) {
			to = from.Add(day60)
		}

		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceUserSettings,
			Generator:  newInRange(from, to),
		}, nil
	}

	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceReasonColumnType,
		Generator:  newAlwaysNow(),
	}, nil
}
