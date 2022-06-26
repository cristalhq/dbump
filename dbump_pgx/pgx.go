package dbump_pgx

import (
	"context"
	"errors"
	"fmt"

	"github.com/cristalhq/dbump"
	"github.com/jackc/pgx/v4"
)

// to prevent multiple migrations running at the same time
const lockNum int64 = 777_777_777

var _ dbump.Migrator = &Migrator{}

// Migrator to migrate Postgres.
type Migrator struct {
	conn      *pgx.Conn
	tx        pgx.Tx
	tableName string
}

// Config for the migrator.
type Config struct {
	// Schema for the dbump version table. Default is empty which means "public" schema.
	Schema string
	// Table for the dbump version table. Default is empty which means "_dbump_log" table.
	Table string
}

// NewMigrator instantiates new Migrator.
func NewMigrator(conn *pgx.Conn, cfg Config) *Migrator {
	if cfg.Schema != "" {
		cfg.Schema += "."
	}
	if cfg.Table == "" {
		cfg.Table = "_dbump_log"
	}
	return &Migrator{
		conn:      conn,
		tableName: cfg.Schema + cfg.Table,
	}
}

// Init is a method from Migrator interface.
func (pg *Migrator) Init(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	version    BIGINT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL
);`, pg.tableName)
	_, err := pg.conn.Exec(ctx, query)
	return err
}

// LockDB is a method from Migrator interface.
func (pg *Migrator) LockDB(ctx context.Context) error {
	_, err := pg.conn.Exec(ctx, "SELECT pg_advisory_lock($1);", lockNum)
	return err
}

// UnlockDB is a method from Migrator interface.
func (pg *Migrator) UnlockDB(ctx context.Context) error {
	_, err := pg.conn.Exec(ctx, "SELECT pg_advisory_unlock($1);", lockNum)
	return err
}

// Version is a method from Migrator interface.
func (pg *Migrator) Version(ctx context.Context) (version int, err error) {
	query := fmt.Sprintf("SELECT version FROM %s ORDER BY created_at DESC LIMIT 1;", pg.tableName)
	var row pgx.Row
	if pg.tx != nil {
		row = pg.tx.QueryRow(ctx, query)
	} else {
		row = pg.conn.QueryRow(ctx, query)
	}
	err = row.Scan(&version)
	if err != nil && errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

// SetVersion is a method from Migrator interface.
func (pg *Migrator) SetVersion(ctx context.Context, version int) error {
	query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES ($1, NOW());", pg.tableName)
	var err error
	if pg.tx != nil {
		_, err = pg.tx.Exec(ctx, query, version)
	} else {
		_, err = pg.conn.Exec(ctx, query, version)
	}
	return err
}

// Begin is a method from Migrator interface.
func (pg *Migrator) Begin(ctx context.Context) error {
	var err error
	pg.tx, err = pg.conn.Begin(ctx)
	return err
}

// Commit is a method from Migrator interface.
func (pg *Migrator) Commit(ctx context.Context) error {
	return pg.tx.Commit(ctx)
}

// Rollback is a method from Migrator interface.
func (pg *Migrator) Rollback(ctx context.Context) error {
	return pg.tx.Rollback(ctx)
}

// Exec is a method from Migrator interface.
func (pg *Migrator) Exec(ctx context.Context, query string, args ...interface{}) error {
	var err error
	if pg.tx != nil {
		_, err = pg.tx.Exec(ctx, query, args...)
	} else {
		_, err = pg.conn.Exec(ctx, query, args...)
	}
	return err
}
