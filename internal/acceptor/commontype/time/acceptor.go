package time

import (
	"context"
	"fmt"
	"time"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
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
	const fnName = "common type time: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Timestamp {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceReasonColumnType,
		Generator: timestamp.NewInRangeGenerator(
			time.Now().Add(-time.Hour*24*60),
			time.Now().Add(time.Hour*24*60),
		),
		ChooseCallback: nil,
	}, nil
}
