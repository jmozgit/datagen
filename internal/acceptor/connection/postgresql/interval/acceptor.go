package interval

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/interval"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Provider struct{}

func NewProvider() *Provider {
	return &Provider{}
}

func (p *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql interval: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.SourceType != "interval" {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
		Generator:      interval.NewPostgresql(),
		ChooseCallback: nil,
	}, nil
}
