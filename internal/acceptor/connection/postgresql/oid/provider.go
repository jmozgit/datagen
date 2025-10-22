package oid

import (
	"context"
	"fmt"

	"github.com/alecthomas/units"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/ahead"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/oid"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Provider struct {
	pool *pgxpool.Pool
}

func NewProvider(pool *pgxpool.Pool) Provider {
	return Provider{
		pool: pool,
	}
}

func (s Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	baseType, ok := req.BaseType.Get()
	if !ok || baseType.SourceType != "oid" {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: oid", contract.ErrGeneratorDeclined)
	}

	defaultSize := units.MB * 5
	rangeValue := units.KB * 250

	userSettings, userOk := req.UserSettings.Get()
	if userOk && userSettings.LO != nil {
		if userSettings.LO.Size != 0 {
			defaultSize = units.MetricBytes(userSettings.LO.Size)
		}
		if userSettings.LO.Range != 0 {
			rangeValue = units.MetricBytes(userSettings.LO.Range)
		}
	}

	return model.AcceptanceDecision{
		Generator:      ahead.NewGenerator(oid.NewApproximatelySizedGenerator(s.pool, int64(defaultSize), int64(rangeValue))),
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
		ChooseCallback: nil,
	}, nil

}
