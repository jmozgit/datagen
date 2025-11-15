package goora

import (
	_ "github.com/sijms/go-ora/v2"
)

// type adapter struct {
// 	*sqlx.DB
// }

// // func NewAdapterPool(
// 	ctx context.Context,
// 	dsn string,
// ) (db.Connect, error) {
// 	const fnName = "goora: new adapter pool"

// 	db, err := sqlx.Open("oracle", dsn)
// 	if err != nil {
// 		return nil, fmt.Errorf("%w: %s", err, fnName)
// 	}

// 	return &adapter{DB: db}, nil
// }

// func (a *adapter) Close(ctx context.Context) error {
// 	return a.DB.Close()
// }
