package numeric

import (
	"context"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

type connect interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Provider struct {
	connect connect
}

func NewProvider(connect connect) *Provider {
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
	tableName, err := model.TableNameFromIdentifier(dataset.ID)
	if err != nil {
		return numericTemplate{}, fmt.Errorf("%w: %s", err, fnName)
	}

	row := p.connect.QueryRow(ctx, query, tableName.Schema, tableName.Table, string(column.SourceName))

	var (
		prec  *int
		scale *int
	)

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
	dataset model.DatasetSchema,
	_ mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (model.AcceptanceDecision, error) {
	const fnName = "numeric: accept"

	baseType, ok := optBaseType.Get()
	if !ok || baseType.SourceType != "numeric" {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	template, err := p.getNumericTemplate(ctx, dataset, baseType)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceReasonDriverAwareance,
		Generator:  newPGNumericGenerator(template),
	}, nil
}
