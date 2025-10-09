package uuid

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/uuid"
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
	const fnName = "common type uuid: accept"
	baseType, ok := req.BaseType.Get()

	if !ok || baseType.Type != model.UUID {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceReasonColumnType,
		Generator:      uuid.NewUUIDV4Generator(),
		ChooseCallback: nil,
	}, nil
}
