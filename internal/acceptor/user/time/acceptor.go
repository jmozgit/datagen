package time

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator/timestamp"
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
	const fnName = "user time: accept"

	userSettings, ok := req.UserSettings.Get()
	if !ok || userSettings.Type != config.GeneratorTypeTimestamp {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}
	timestampSetting := userSettings.Timestamp

	if timestampSetting == nil {
		from, to := time.Now().AddDate(0, -1, 0), time.Now().AddDate(0, 1, 0)
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceUserSettings,
			Generator:  timestamp.NewInRangeGenerator(from, to),
		}, nil
	}

	if timestampSetting.OnlyNow {
		return model.AcceptanceDecision{
			AcceptedBy: model.AcceptanceUserSettings,
			Generator:  timestamp.NewAlwaysNowGenerator(),
		}, nil
	}

	const days60 = time.Hour * 24 * 60

	from := lo.FromPtrOr(timestampSetting.From, time.Now().Add(-days60))
	to := lo.FromPtrOr(timestampSetting.To, time.Now().Add(days60))

	if to.Before(from) {
		to = from.Add(days60)
	}

	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceUserSettings,
		Generator:  timestamp.NewInRangeGenerator(from, to),
	}, nil
}
