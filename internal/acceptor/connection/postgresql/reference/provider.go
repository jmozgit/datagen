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
)

type Provider struct {
	connect db.Connect
	refsvc  model.ReferenceResolver
}

func NewProvider(
	connect db.Connect,
	refsvc model.ReferenceResolver,
) *Provider {
	return &Provider{
		connect: connect,
		refsvc:  refsvc,
	}
}

type referenceInfo struct {
	table  model.Identifier
	column model.Identifier
}

func (p *Provider) resolveReference(
	ctx context.Context,
	schema model.DatasetSchema,
	baseType model.TargetType,
) (referenceInfo, error) {
	const fnName = "resolve reference"

	const query = `
	SELECT
    	nsp.nspname AS references_schema,
    	confrelid::regclass AS references_table,
    	af.attname AS references_column
	FROM pg_constraint AS c
	JOIN pg_attribute AS a 
    	ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
	JOIN pg_attribute AS af 
		ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid
	JOIN pg_class AS cl 
		ON cl.oid = c.confrelid
	JOIN pg_namespace AS nsp 
		ON nsp.oid = cl.relnamespace
	WHERE 
    	c.contype = 'f' AND c.conrelid = $1::regclass AND a.attname = $2;
	`

	// scan identifier must be smarter
	var (
		tableName  model.TableName
		columnName model.Identifier
	)

	err := p.connect.
		QueryRow(ctx, query, schema.ID, baseType.SourceName).
		Scan(&tableName.Schema, &tableName.Table, &columnName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return referenceInfo{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
		}

		return referenceInfo{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return referenceInfo{
		table:  model.Identifier(tableName.String()),
		column: columnName,
	}, nil
}

func (p *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql reference: accept"

	baseType, ok := req.BaseType.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	refInfo, err := p.resolveReference(ctx, req.Dataset, baseType)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	reader, err := reader.NewConnection(refInfo.table, refInfo.column, 150, p.connect)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	generator, chooseCallback := reference.NewBufferedValuesGenerator(
		req.Dataset,
		refInfo.table, refInfo.column,
		reader, p.refsvc, 100,
	)

	return model.AcceptanceDecision{
		Generator:      generator,
		ChooseCallback: chooseCallback,
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
	}, nil
}
