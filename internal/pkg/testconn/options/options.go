package options

type PartPolicy struct {
	Method string
	Cnt    int
	Field  string
}

type CreateTableOptions struct {
	PKs        []string
	PartPolicy PartPolicy
	Preserve   bool
}

type CreateTableOption func(c *CreateTableOptions)

func WithPKs(pks []string) CreateTableOption {
	return func(c *CreateTableOptions) {
		c.PKs = pks
	}
}

func WithHashPartitions(parts int, field string) CreateTableOption {
	return func(c *CreateTableOptions) {
		c.PartPolicy = PartPolicy{
			Method: "hash",
			Cnt:    parts,
			Field:  field,
		}
	}
}

func WithPreserve() CreateTableOption {
	return func(c *CreateTableOptions) {
		c.Preserve = true
	}
}
