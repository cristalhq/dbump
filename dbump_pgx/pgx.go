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
	row := pg.conn.QueryRow(ctx, query)
	err = row.Scan(&version)
	if err != nil && errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

// Version is a method from Migrator interface.
func (pg *Migrator) DoStep(ctx context.Context, step dbump.Step) error {
	if step.DisableTx {
		if _, err := pg.conn.Exec(ctx, step.Query); err != nil {
			return err
		}
		query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES ($1, NOW());", pg.cfg.tableName)
		_, err := pg.conn.Exec(ctx, query, step.Version)
		return err
	}

	return pg.conn.BeginFunc(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, step.Query); err != nil {
			return err
		}
		query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES ($1, NOW());", pg.cfg.tableName)
		_, err := tx.Exec(ctx, query, step.Version)
		return err
	})
}
