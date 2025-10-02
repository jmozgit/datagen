package serail

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/generator/postgresql/serial"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
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

	tableName, err := model.TableNameFromIdentifier(dataset.ID)
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, fnName)
	}

	query := fmt.Sprintf("select pg_get_serial_sequence('%s', '%s')", tableName.String(), string(baseType.SourceName))

	var seqName sql.NullString
	if err := s.conn.QueryRow(ctx, query).Scan(&seqName); err != nil {
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
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	seqName, err := s.getSeqName(ctx, req.Dataset, baseType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
		}

		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceReasonDriverAwareness,
		Generator:  serial.NewSeqBasedGenerator(s.conn, seqName),
	}, nil
}
