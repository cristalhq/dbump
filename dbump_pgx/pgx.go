package dbump_pgx

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/cristalhq/dbump"
	"github.com/jackc/pgx/v4"
)

var _ dbump.Migrator = &Migrator{}

// Migrator to migrate Postgres.
type Migrator struct {
	conn *pgx.Conn
	tx   pgx.Tx
	cfg  Config
}

// Config for the migrator.
type Config struct {
	// Schema for the dbump version table. Default is empty which means "public" schema.
	Schema string
	// Table for the dbump version table. Default is empty which means "_dbump_log" table.
	Table string

	// [schema].table
	tableName string
	// to prevent multiple migrations running at the same time
	lockNum int64
}

// NewMigrator instantiates new Migrator.
func NewMigrator(conn *pgx.Conn, cfg Config) *Migrator {
	if cfg.Schema != "" {
		cfg.tableName += cfg.Schema + "."
	}
	if cfg.Table == "" {
		cfg.Table = "_dbump_log"
	}
	cfg.tableName += cfg.Table

	h := fnv.New64()
	h.Write([]byte(cfg.tableName))
	cfg.lockNum = int64(h.Sum64())

	return &Migrator{
		conn: conn,
		cfg:  cfg,
	}
}

// Init is a method from Migrator interface.
func (pg *Migrator) Init(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;
CREATE TABLE IF NOT EXISTS %s (
	version    BIGINT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL
);`, pg.cfg.Schema, pg.cfg.tableName)
	_, err := pg.conn.Exec(ctx, query)
	return err
}

// LockDB is a method from Migrator interface.
func (pg *Migrator) LockDB(ctx context.Context) error {
	_, err := pg.conn.Exec(ctx, "SELECT pg_advisory_lock($1);", pg.cfg.lockNum)
	return err
}

// UnlockDB is a method from Migrator interface.
func (pg *Migrator) UnlockDB(ctx context.Context) error {
	_, err := pg.conn.Exec(ctx, "SELECT pg_advisory_unlock($1);", pg.cfg.lockNum)
	return err
}

// Version is a method from Migrator interface.
func (pg *Migrator) Version(ctx context.Context) (version int, err error) {
	query := fmt.Sprintf("SELECT version FROM %s ORDER BY created_at DESC LIMIT 1;", pg.cfg.tableName)
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
	query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES ($1, NOW());", pg.cfg.tableName)
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
