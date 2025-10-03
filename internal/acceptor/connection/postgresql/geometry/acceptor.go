package geometry

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/box"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/circle"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/line"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/path"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/point"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/polygon"
	"github.com/viktorkomarov/datagen/internal/model"
)

type Provider struct{}

func NewProvider() Provider {
	return Provider{}
}

var geometryGenerator = map[string]model.Generator{
	"box":     box.NewPostgresql(),
	"point":   point.NewPostgresql(),
	"polygon": polygon.NewPostgresql(15),
	"path":    path.NewPostgresql(10),
	"line":    line.NewPostgresql(),
	"circle":  circle.NewPostgresql(),
}

func (p Provider) Accept(
	_ context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql geometry: accept"

	baseType, ok := req.BaseType.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	gen, ok := geometryGenerator[baseType.SourceType]
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceReasonDriverAwareness,
		Generator:  gen,
	}, nil
}
