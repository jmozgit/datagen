package serial

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/config"
	"github.com/viktorkomarov/datagen/internal/generator"
	"github.com/viktorkomarov/datagen/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/mo"
)

type Provider struct {
	pool *pgxpool.Pool
}

func NewProvider(pool *pgxpool.Pool) *Provider {
	return &Provider{pool: pool}
}

func (s *Provider) getSeqName(
	ctx context.Context,
	dataset model.DatasetSchema,
	baseType model.TargetType,
) (string, error) {
	tableName, err := model.TableNameFromIdentifier(dataset.ID)
	if err != nil {
		return "", fmt.Errorf("%w: get seq name", err)
	}

	query := fmt.Sprintf("select pg_get_serial_sequence(%s, %s)", tableName.String(), string(baseType.SourceName))

	var seqName sql.NullString
	if err := s.pool.QueryRow(ctx, query).Scan(&seqName); err != nil {
		return "", fmt.Errorf("%w: get seq name", err)
	}
	if !seqName.Valid {
		return "", fmt.Errorf("%w: get seq name", sql.ErrNoRows)
	}

	return seqName.String, nil
}

func (s *Provider) Accept(
	ctx context.Context,
	dataset model.DatasetSchema,
	optUserSettings mo.Option[config.Generator],
	optBaseType mo.Option[model.TargetType],
) (model.AcceptanceDecision, error) {
	const fnName = "serial: accept"

	userSettings, ok := optUserSettings.Get()
	if ok && userSettings.Type != config.GeneratorTypeInteger {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	if ok && userSettings.Integer.Format != nil && *userSettings.Integer.Format != "serial" {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	baseType, ok := optBaseType.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
	}

	seqName, err := s.getSeqName(ctx, dataset, baseType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", generator.ErrGeneratorDeclined, fnName)
		}

		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	return model.AcceptanceDecision{
		AcceptedBy: model.AcceptanceReasonColumnType,
		Generator:  &seqGenerator{seqName: seqName, pool: s.pool},
	}, nil
}
