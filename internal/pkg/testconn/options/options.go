package options

import "github.com/jmozgit/datagen/internal/pkg/db"

type PartPolicy struct {
	Method string
	Cnt    int
	Field  string
}

type CreateTableOptions struct {
	PKs        []string
	PartPolicy PartPolicy
	Preserve   bool
	FGs        string
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

func WithForeignKey(fg string) CreateTableOption {
	return func(c *CreateTableOptions) {
		c.FGs = fg
	}
}

type OnEachRow struct {
	ScanFn func(row db.Row) ([]any, error)
}

type OnEachRowOption func(o *OnEachRow)

func WithScanFn(scanFn func(row db.Row) ([]any, error)) OnEachRowOption {
	return func(o *OnEachRow) {
		o.ScanFn = scanFn
	}
}
