package text

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/text"
	"github.com/jmozgit/datagen/internal/model"
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "common type text: accept"
	baseType, ok := req.BaseType.Get()

	if !ok || baseType.Type != model.Text {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceReasonColumnType,
		Generator:      text.NewInRangeSizeGenerator(20, 100),
		ChooseCallback: nil,
	}, nil
}
