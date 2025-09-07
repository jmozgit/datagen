package model

type Identifier string

type DatasetSchema struct {
	ID    Identifier
	Types []Type
}

type Type struct {
}
