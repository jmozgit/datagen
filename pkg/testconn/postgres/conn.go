package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/samber/lo"
)

type Conn struct {
	conn *pgx.Conn
}

func New(ctx context.Context, t *testing.T, connStr string) (*Conn, error) {
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("%w parse config", err)
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: postgres new", err)
	}
	defer conn.Close(ctx)

	dbname, err := createTempDb(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("%w: postgres new", err)
	}

	cfg.Database = dbname
	tempConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: connect config", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		_, err := tempConn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", dbname))
		if err != nil {
			t.Errorf("failed to drop database %s: %v", dbname, err)
		}
		if err = tempConn.Close(ctx); err != nil {
			t.Errorf("failed to close temp conn: %v", err)
		}
	})

	return &Conn{conn: tempConn}, nil
}

func createTempDb(ctx context.Context, conn *pgx.Conn) (string, error) {
	dbname := genDbName()
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbname))
	if err != nil {
		return "", fmt.Errorf("%w: create temp db", err)
	}

	return dbname, nil
}

func (c *Conn) Raw() *pgx.Conn {
	return c.conn
}

func (c *Conn) CreateTable(ctx context.Context, name string, cols [][2]string, pks []string) error {
	if err := c.ensureUnexistence(ctx, name); err != nil {
		return fmt.Errorf("%w: ensure unexistence %s", err, name)
	}

	query := "create table %s ("
	query += strings.Join(lo.Map(cols, func(part [2]string, _ int) string {
		return fmt.Sprintf("%s %s", part[0], part[1])
	}), ",")
	query += ")"

	query = fmt.Sprintf(query, name)

	_, err := c.conn.Exec(ctx, query)
	return err
}

func (c *Conn) ensureUnexistence(ctx context.Context, name string) error {
	_, err := c.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	return err
}

func genDbName() string {
	return randStr(10)
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func randStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
