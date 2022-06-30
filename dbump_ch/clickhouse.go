package dbump_ch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cristalhq/dbump"
)

var _ dbump.Migrator = &Migrator{}

// Migrator to migrate ClickHouse.
type Migrator struct {
	conn *sql.DB
	cfg  Config
}

// Config for the migrator.
type Config struct {
	// Database for the dbump version table. Default is empty.
	Database string
	// Table for the dbump version table. Default is empty which means "_dbump_log" table.
	Table string
	// OnCluster
	OnCluster bool
	// Engine
	Engine string

	tableName string
}

// NewMigrator instantiates new Migrator.
func NewMigrator(conn *sql.DB, cfg Config) *Migrator {
	if cfg.Database != "" {
		cfg.Database += "."
	}
	if cfg.Table == "" {
		cfg.Table = "_dbump_log"
	}
	if cfg.Engine == "" {
		cfg.Engine = "TinyLog"
	}
	cfg.tableName = cfg.Database + cfg.Table

	return &Migrator{
		conn: conn,
		cfg:  cfg,
	}
}

// Init is a method from Migrator interface.
func (ch *Migrator) Init(ctx context.Context) error {
	withCluster := ""
	if ch.cfg.OnCluster {
		withCluster = " ON CLUSTER"
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s%s (
	version    BIGINT NOT NULL,
	created_at TIMESTAMP NOT NULL
) ENGINE = %s;`, ch.cfg.tableName, withCluster, ch.cfg.Engine)
	_, err := ch.conn.ExecContext(ctx, query)
	return err
}

// LockDB is a method from Migrator interface.
func (ch *Migrator) LockDB(ctx context.Context) error { return nil }

// UnlockDB is a method from Migrator interface.
func (ch *Migrator) UnlockDB(ctx context.Context) error { return nil }

// Version is a method from Migrator interface.
func (ch *Migrator) Version(ctx context.Context) (version int, err error) {
	query := fmt.Sprintf("SELECT version FROM %s ORDER BY created_at DESC LIMIT 1;", ch.cfg.tableName)
	row := ch.conn.QueryRowContext(ctx, query)
	err = row.Scan(&version)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (ch *Migrator) DoStep(ctx context.Context, step dbump.Step) error {
	tx, err := ch.conn.Begin()
	if err != nil {
		return err
	}
	// TODO: rollback

	if _, err := tx.ExecContext(ctx, step.Query); err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s (version, created_at) VALUES (?, ?);", ch.cfg.tableName)
	if _, err := tx.ExecContext(ctx, query, step.Version, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}
