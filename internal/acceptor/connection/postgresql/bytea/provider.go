package bytea

import (
	"context"
	"fmt"

	"github.com/c2h5oh/datasize"
	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/bytea"
	"github.com/jmozgit/datagen/internal/model"
)

type Provider struct {
}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "bytea: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.SourceType != "bytea" {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	dfltSize := datasize.KB * 100
	diff := datasize.KB * 20

	return model.AcceptanceDecision{
		ChooseCallback: nil,
		Generator:      bytea.NewAroundByteaGenerator(dfltSize, diff),
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
	}, nil
}
