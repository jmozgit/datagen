package date

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
	const fnName = "common type date: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Date {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	nowY := time.Now().Year()
	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceReasonColumnType,
		Generator: timestamp.NewInRangeGenerator(
			time.Date(nowY-2, time.January, 1, 0, 0, 0, 0, time.UTC),
			time.Date(nowY+2, time.January, 1, 0, 0, 0, 0, time.UTC),
		),
	}, nil
}
