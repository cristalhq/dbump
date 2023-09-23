package dbump_pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/cristalhq/dbump"
)

var _ dbump.Migrator = &Migrator{}

// Migrator to migrate Postgres.
type Migrator struct {
	conn *sql.DB
	cfg  Config
}

// Config for the migrator.
type Config struct {
	// Schema for the dbump version table. Default is empty which means "public" schema.
	Schema string
	// Table for the dbump version table. Default is empty which means "_dbump_log" table.
	Table string

	// [schema.]table
	tableName string
	// to prevent multiple migrations running at the same time
	lockNum int64
}

// NewMigrator instantiates new Migrator.
// Takes std *sql.DB.
func NewMigrator(conn *sql.DB, cfg Config) *Migrator {
	if cfg.Schema == "" {
		cfg.Schema = "public"
	}
	if cfg.Table == "" {
		cfg.Table = "_dbump_log"
	}

	cfg.tableName = cfg.Schema + "." + cfg.Table
	cfg.lockNum = hashTableName(cfg.tableName)

	return &Migrator{
		conn: conn,
		cfg:  cfg,
	}
}

// Init migrator.
func (pg *Migrator) Init(ctx context.Context) error {
	var query string
	if pg.cfg.Schema != "" {
		query = fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, pg.cfg.Schema)
	}

	query += fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	version    BIGINT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL
);`, pg.cfg.tableName)

	_, err := pg.conn.ExecContext(ctx, query)
	return err
}

// Drop is a method from Migrator interface.
func (pg *Migrator) Drop(ctx context.Context) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, pg.cfg.tableName)
	_, err := pg.conn.ExecContext(ctx, query)
	return err
}

// LockDB is a method from Migrator interface.
func (pg *Migrator) LockDB(ctx context.Context) error {
	_, err := pg.conn.ExecContext(ctx, "SELECT pg_advisory_lock($1);", pg.cfg.lockNum)
	return err
}

// UnlockDB is a method from Migrator interface.
func (pg *Migrator) UnlockDB(ctx context.Context) error {
	_, err := pg.conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1);", pg.cfg.lockNum)
	return err
}

// Version is a method for Migrator interface.
func (pg *Migrator) Version(ctx context.Context) (version int, err error) {
	query := fmt.Sprintf("SELECT version FROM %s ORDER BY created_at DESC LIMIT 1;", pg.cfg.tableName)
	row := pg.conn.QueryRowContext(ctx, query)
	err = row.Scan(&version)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

// DoStep is a method for Migrator interface.
func (pg *Migrator) DoStep(ctx context.Context, step dbump.Step) error {
	if step.DisableTx {
		if _, err := pg.conn.ExecContext(ctx, step.Query); err != nil {
			return err
		}
		query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES ($1, NOW());", pg.cfg.tableName)
		_, err := pg.conn.ExecContext(ctx, query, step.Version)
		return err
	}

	return pg.beginFunc(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, step.Query); err != nil {
			return err
		}
		query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES ($1, NOW());", pg.cfg.tableName)
		_, err := tx.ExecContext(ctx, query, step.Version)
		return err
	})
}

func (pg *Migrator) beginFunc(ctx context.Context, f func(*sql.Tx) error) (err error) {
	tx, err := pg.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := f(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func hashTableName(s string) int64 {
	h := fnv.New64()
	h.Write([]byte(s))
	return int64(h.Sum64())
}
