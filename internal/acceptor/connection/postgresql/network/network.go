package network

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/inet"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/macaddr"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

var networkGeometry = map[string]model.Generator{
	"inet":     inet.NewPostgresql(),
	"cidr":     inet.NewPostgresql(),
	"macaddr":  macaddr.NewPostgresql(),
	"macaddr8": macaddr.NewPostgresql(),
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql network: accept"

	baseType, ok := req.BaseType.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	gen, ok := networkGeometry[baseType.SourceType]
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceReasonDriverAwareness,
		Generator:  gen,
	}, nil
}
