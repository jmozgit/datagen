package text

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/text"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
)

type Provider struct {
	conn db.Connect
}

func NewProvider(conn db.Connect) *Provider {
	return &Provider{conn: conn}
}

func (s *Provider) getTextSize(
	ctx context.Context,
	dataset model.DatasetSchema,
	baseType model.TargetType,
) (int64, error) {
	const fnName = "get text size"

	tableName := dataset.TableName

	const query = `
	SELECT
    	character_maximum_length
	FROM information_schema.columns s
	WHERE s.table_schema = $1
  		AND s.table_name = $2
  		AND s.column_name = $3
	`

	var size sql.NullInt64
	if err := s.conn.QueryRow(
		ctx, query,
		tableName.Schema.AsArgument(), tableName.Table.AsArgument(),
		baseType.SourceName.AsArgument(),
	).Scan(&size); err != nil {
		return 0, fmt.Errorf("%w: %s", err, fnName)
	}
	if !size.Valid {
		return 0, nil
	}

	return size.Int64, nil
}

func (s *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql text: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Text {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	if !slices.Contains([]string{"bpchar", "varchar"}, baseType.SourceType) {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	size, err := s.getTextSize(ctx, req.Dataset, baseType)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	var gen model.Generator
	if size == 0 {
		gen = text.NewInRangeSizeGenerator(20, 120)
	} else {
		gen = text.NewFixedSizedStringGenerator(int(size))
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
		Generator:      gen,
		ChooseCallback: nil,
	}, nil
}
