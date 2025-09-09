package model

type Identifier string

type DatasetSchema struct {
	ID                Identifier
	DataTypes         []BaseType
	UniqueConstraints []UniqueConstraints
}
