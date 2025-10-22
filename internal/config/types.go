package config

type GeneratorType string

const (
	GeneratorTypeInteger         GeneratorType = "integer"
	GeneratorTypeFloat           GeneratorType = "float"
	GeneratorTypeTimestamp       GeneratorType = "timestamp"
	GeneratorTypeUUID            GeneratorType = "uuid"
	GeneratorTypeLua             GeneratorType = "lua"
	GeneratorTypeProbabilityList GeneratorType = "list_probability"
	GeneratorTypeText            GeneratorType = "text"
	GeneratorTypeLO              GeneratorType = "lo"
)
