package enum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/viktorkomarov/datagen/internal/acceptor/contract"
	"github.com/viktorkomarov/datagen/internal/generator/oneof"
	"github.com/viktorkomarov/datagen/internal/model"
	"github.com/viktorkomarov/datagen/internal/pkg/db"
)

type Provider struct {
	connect db.Connect
}

func NewProvider(connect db.Connect) *Provider {
	return &Provider{
		connect: connect,
	}
}

func (p *Provider) enumOID(ctx context.Context, rawType string) (uint32, error) {
	const fnName = "enum oid"

	const query = "SELECT oid FROM pg_type WHERE typname = $1 AND typtype = 'e'"

	var oid uint32
	err := p.connect.QueryRow(ctx, query, rawType).Scan(&oid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
		}

		return 0, fmt.Errorf("%w: %s", err, fnName)
	}

	return oid, nil
}

func (p *Provider) getEnums(ctx context.Context, baseType model.TargetType) ([]string, error) {
	const fnName = "get enums"

	enumOID, err := p.enumOID(ctx, baseType.SourceType)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	const query = "SELECT enumlabel FROM pg_enum WHERE enumtypid = $1"
	rows, err := p.connect.Query(ctx, query, enumOID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}
	defer rows.Close()

	enums := make([]string, 0)
	for rows.Next() {
		var enum string

		if err := rows.Scan(&enum); err != nil {
			return nil, fmt.Errorf("%w: %s", err, fnName)
		}

		enums = append(enums, enum)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, fnName)
	}

	return enums, nil
}

func (p *Provider) Accept(
	ctx context.Context,
	req contract.AcceptRequest,
) (model.AcceptanceDecision, error) {
	const fnName = "postgresql enum: accept"

	baseType, ok := req.BaseType.Get()
	if !ok {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	enums, err := p.getEnums(ctx, baseType)
	if err != nil {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", err, fnName)
	}

	if len(enums) == 0 {
		return model.AcceptanceDecision{}, fmt.Errorf("%w: %s", contract.ErrGeneratorDeclined, fnName)
	}

	return model.AcceptanceDecision{
		Generator:      oneof.NewGenerator(enums),
		AcceptedBy:     model.AcceptanceReasonDriverAwareness,
		ChooseCallback: nil,
	}, nil
}
