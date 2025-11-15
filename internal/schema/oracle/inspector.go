package oracle

import (
	_ "github.com/sijms/go-ora/v2"

	"github.com/jmozgit/datagen/internal/pkg/db"
)

type Inspector struct {
	connect db.Connect
}

// func NewInspector(ctx context.Context, connCfg *config.SQLConnection) (*Inspector, error) {
// 	const fnName = "oracle: new inspector"

// 	dsn := connCfg.ConnString("oracle")
// 	connect, err := goora.NewAdapterPool(ctx, dsn)
// 	if err != nil {
// 		return nil, fmt.Errorf("%w: %s", err, fnName)
// 	}

// 	return &Inspector{connect: connect}, nil
// }

// func (i *Inspector) Close(ctx context.Context) error {
// 	const fnName = "oracle: close"

// 	if err := i.connect.Close(ctx); err != nil {
// 		return fmt.Errorf("%w: %s", err, fnName)
// 	}

// 	return nil
// }
