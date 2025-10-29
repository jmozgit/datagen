package numeric

import (
	"context"
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/postgresql/numeric"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/db"
	"github.com/samber/lo"
)

type Provider struct {
	connect db.Connect
}

func NewProvider(connect db.Connect) *Provider {
	return &Provider{
		connect: connect,
	}
}

type numericTemplate struct {
	scale     int
	precision int
}

func (p *Provider) getNumericTemplate(
	ctx context.Context,
	dataset model.DatasetSchema,
	column model.TargetType,
) (numericTemplate, error) {
	const fnName = "get numeric template"

	const query = `
		SELECT 
			numeric_precision, numeric_scale
		FROM
			information_schema.columns
		WHERE
			table_schema = $1 AND table_name = $2 AND column_name = $3
		`
	tableName := dataset.TableName

	var (
		prec  *int
		scale *int
	)

	row := p.connect.QueryRow(ctx, query, tableName.Schema.AsArgument(), tableName.Table.AsArgument(), column.SourceName.AsArgument())
	if err := row.Scan(&prec, &scale); err != nil {
		return numericTemplate{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return numericTemplate{
		precision: lo.FromPtrOr(prec, 0),
		scale:     lo.FromPtrOr(scale, 0),
	}, nil
}

func (p *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql numeric: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.SourceType != "numeric" {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	template, err := p.getNumericTemplate(ctx, req.Dataset, baseType)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
		Generator:      numeric.NewPostgresqlNumericGenerator(template.scale, template.precision),
		ChooseCallback: nil,
	}, nil
}
