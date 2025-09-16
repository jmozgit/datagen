package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

func IsConstraintViolatesErr(err error) bool {
	var pgxErr *pgconn.PgError
	if errors.As(err, &pgxErr) {
		return pgxErr.Code == "23514" || pgxErr.Code == "23505"
	}

	return false
}
