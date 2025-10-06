package reference

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/connection/postgresql/reference/reader"
	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/reference"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
	"github.com/viktorkomarov/datagen/internal/refresolver"
)

type Provider struct {
	connect db.Connect
	refsvc  *refresolver.Service
}

func NewProvider(
	connect db.Connect,
	refsvc *refresolver.Service,
) *Provider {
	return &Provider{
		connect: connect,
		refsvc:  refsvc,
	}
}

type referenceInfo struct {
	targetID model.Identifier
	cellID   model.Identifier
}

func (p *Provider) resolveReference(
	ctx context.Context,
	schema model.DatasetSchema,
	baseType model.TargetType,
) (referenceInfo, error) {
	const fnName = "resolve reference"

	const query = `
	SELECT
    	confrelid::regclass AS references_table,
    	af.attname AS references_column,
    FROM pg_constraint AS c
	JOIN 
		pg_attribute 
	AS a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
	JOIN
		pg_attribute
	AS af ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid
	WHERE 
		c.contype = 'f' AND c.conrelid = $1::regclass AND a.attname = $2
	`

	// scan identifier must be smarter
	var refInfo referenceInfo
	err := p.connect.
		QueryRow(ctx, query, schema.ID, baseType.SourceName).
		Scan(&refInfo.targetID, &refInfo.cellID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return referenceInfo{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
		}

		return referenceInfo{}, nil
	}

	return refInfo, nil
}

func (p *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql reference: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Reference {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	refInfo, err := p.resolveReference(ctx, req.Dataset, baseType)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	reader, err := reader.NewConnection(req.Dataset, refInfo.cellID, 150, p.connect)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		Generator: reference.NewBufferedValuesGenerator(
			refInfo.targetID, refInfo.cellID,
			reader, 100,
		),
		AcceptedBy: model.AcceptanceReasonDriverAwareness,
	}, nil
}
