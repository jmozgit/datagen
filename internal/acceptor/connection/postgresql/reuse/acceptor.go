package reuse

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/connection/postgresql/reference/reader"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/reuse"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/db"
)

type Provider struct {
	connect db.Connect
}

func NewProvider(
	connect db.Connect,
) *Provider {
	return &Provider{
		connect: connect,
	}
}

func (p *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql reuse: accept"

	baseType, ok := req.BaseType.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	baseGen, ok := req.BaseGenerator.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	settings, ok := req.UserSettings.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	reader := reader.NewConnection(
		req.Dataset.TableName,
		baseType.SourceName, 150, p.connect,
	)

	gen, err := reuse.NewGenerator(ctx, reader, settings.ReuseFraction, baseGen)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		ChooseCallback: nil,
		Generator:      gen,
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
	}, nil
}
