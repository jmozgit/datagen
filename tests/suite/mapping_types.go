package suite

//nolint:gochecknoglobals // ok
var pgMappgingType = map[Type]string{
	TypeInt2:        "int2",
	TypeInt4:        "int4",
	TypeInt8:        "int8",
	TypeSerialInt2:  "smallserial",
	TypeSerialInt4:  "serial",
	TypeSerialInt8:  "bigserial",
	TypeFloat4:      "float",
	TypeFloat8:      "double precision",
	TypeTimestamp:   "timestamptz",
	TypeBoolean:     "boolean",
	TypeText:        "text",
	TypeBytea:       "bytea",
	TypeArrayInt:    "int[]",
	TypeArrayString: "text[]",
	TypeLO:          "oid",
}
