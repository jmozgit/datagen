package oid

import (
	"context"
	"fmt"

	"github.com/c2h5oh/datasize"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/ahead"
	"github.com/jmozgit/datagen/internal/generator/postgresql/oid"
	"github.com/jmozgit/datagen/internal/model"
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

	defaultSize := datasize.MB * 5
	rangeValue := datasize.ByteSize(0)

	userSettings, userOk := req.UserSettings.Get()
	if userOk && userSettings.LO != nil {
		if userSettings.LO.Size != 0 {
			defaultSize = userSettings.LO.Size
		}
		if userSettings.LO.Range != 0 {
			rangeValue = userSettings.LO.Range
		}
	}

	return model.AcceptanceDecision{
		Generator:      ahead.NewGenerator(oid.NewApproximatelySizedGenerator(s.pool, int64(defaultSize), int64(rangeValue))),
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
		ChooseCallback: nil,
	}, nil

}
