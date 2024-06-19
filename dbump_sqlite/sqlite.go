package dbump_sqlite

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

	_ struct{} // enforce explicit field names.
}

// NewMigrator instantiates new Migrator.
func NewMigrator(conn *sql.DB, cfg Config) *Migrator {
	if cfg.Schema != "" {
		cfg.Schema += "."
	}
	if cfg.Table == "" {
		cfg.Table = "_dbump_log"
	}

	cfg.tableName = cfg.Schema + cfg.Table
	cfg.lockNum = hashTableName(cfg.tableName)

	return &Migrator{
		conn: conn,
		cfg:  cfg,
	}
}

// Init is a method from dbump.Migrator interface.
func (s *Migrator) Init(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	version    INTEGER NOT NULL,
	created_at TEXT NOT NULL
);`, s.cfg.tableName)

	_, err := s.conn.ExecContext(ctx, query)
	return err
}

// Drop is a method from dbump.Migrator interface.
func (s *Migrator) Drop(ctx context.Context) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, s.cfg.tableName)

	// TODO: probably should ignore error for this query
	// if s.cfg.Schema != "" {
	// 	query = fmt.Sprintf(`DROP SCHEMA IF EXISTS %s RESTRICT;`, s.cfg.Schema)
	// }
	_, err := s.conn.ExecContext(ctx, query)
	return err
}

// LockDB is a method from dbump.Migrator interface.
func (s *Migrator) LockDB(ctx context.Context) error {
	return nil
}

// UnlockDB is a method from dbump.Migrator interface.
func (s *Migrator) UnlockDB(ctx context.Context) error {
	return nil
}

// Version is a method from dbump.Migrator interface.
func (s *Migrator) Version(ctx context.Context) (version int, err error) {
	query := fmt.Sprintf("SELECT version FROM %s ORDER BY created_at DESC LIMIT 1;", s.cfg.tableName)
	row := s.conn.QueryRowContext(ctx, query)
	err = row.Scan(&version)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

// DoStep is a method from dbump.Migrator interface.
func (s *Migrator) DoStep(ctx context.Context, step dbump.Step) error {
	if step.DisableTx {
		if _, err := s.conn.ExecContext(ctx, step.Query); err != nil {
			return err
		}
		query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES ($1, STRFTIME('%%Y-%%m-%%d %%H:%%M:%%f', 'NOW'));", s.cfg.tableName)
		_, err := s.conn.ExecContext(ctx, query, step.Version)
		return err
	}
	return s.inTx(ctx, step)
}

func (s *Migrator) inTx(ctx context.Context, step dbump.Step) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES ($1, STRFTIME('%%Y-%%m-%%d %%H:%%M:%%f', 'NOW'));", s.cfg.tableName)
	_, err = s.conn.ExecContext(ctx, query, step.Version)
	// fmt.Printf("args: %s %d\n", query, step.Version)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	_, err = tx.ExecContext(ctx, step.Query)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func hashTableName(s string) int64 {
	h := fnv.New64()
	h.Write([]byte(s))
	return int64(h.Sum64())
}
