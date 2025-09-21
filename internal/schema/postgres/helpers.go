package postgres

import (
	"database/sql"
	"strings"

	"github.com/viktorkomarov/datagen/internal/model"
)

func isSerialInteger(columnDefault sql.NullString, udtName string) bool {
	return strings.Contains(columnDefault.String, "nextval") && pgRegistryTypes[udtName] == model.Integer
}
