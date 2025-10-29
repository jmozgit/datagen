package serial

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmozgit/datagen/internal/acceptor/contract"
	"github.com/jmozgit/datagen/internal/generator/postgresql/serial"
	"github.com/jmozgit/datagen/internal/model"
	"github.com/jmozgit/datagen/internal/pkg/db"
)

type Provider struct {
	conn db.Connect
}

func NewProvider(conn db.Connect) *Provider {
	return &Provider{conn: conn}
}

func (s *Provider) getSeqName(
	ctx context.Context,
	dataset model.DatasetSchema,
	baseType model.TargetType,
) (string, error) {
	const fnName = "get seq name"

	tableName := dataset.TableName

	const query = `
	SELECT
    	s.relname AS sequence_name
	FROM pg_class s
	JOIN pg_namespace n ON n.oid = s.relnamespace
	JOIN pg_depend d ON d.objid = s.oid
	JOIN pg_class t ON d.refobjid = t.oid
	JOIN pg_attribute a ON a.attnum = d.refobjsubid AND a.attrelid = t.oid
	WHERE s.relkind = 'S'
  		AND n.nspname = $1
  		AND t.relname = $2
  		AND a.attname = $3
	`

	var seqName sql.NullString
	if err := s.conn.QueryRow(
		ctx, query,
		tableName.Schema.AsArgument(), tableName.Table.AsArgument(),
		baseType.SourceName.AsArgument(),
	).Scan(&seqName); err != nil {
		return "", fmt.Errorf("%w: %s", err, fnName)
	}
	if !seqName.Valid {
		return "", fmt.Errorf("%w: %s", sql.ErrNoRows, fnName)
	}

	return seqName.String, nil
}

func (s *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql serial: accept"

	baseType, ok := req.BaseType.Get()
	if !ok || baseType.Type != model.Integer {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	seqName, err := s.getSeqName(ctx, req.Dataset, baseType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
		}

		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
		Generator:      serial.NewSeqBasedGenerator(s.conn, seqName),
		ChooseCallback: nil,
	}, nil
}
