package dbump_postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cristalhq/dbump"
)

// to prevent multiple migrations running at the same time
const lockNum int64 = 707_707_707

var _ dbump.Migrator = &Migrator{}

// Migrator to migrate Postgres.
type Migrator struct {
	conn *sql.DB
}

// NewMigrator instantiates new Migrator.
// Takes std *sql.DB.
func NewMigrator(conn *sql.DB) *Migrator {
	return &Migrator{
		conn: conn,
	}
}

// Init migrator.
func (pg *Migrator) Init(ctx context.Context) error {
	query := `CREATE TABLE IF NOT EXISTS _dbump_schema_version (
	version    BIGINT                   NOT NULL PRIMARY KEY,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL
);`
	_, err := pg.conn.ExecContext(ctx, query)
	return err
}

// LockDB is a method for Migrator interface.
func (pg *Migrator) LockDB(ctx context.Context) error {
	_, err := pg.conn.ExecContext(ctx, "SELECT pg_advisory_lock($1);", lockNum)
	return err
}

// UnlockDB is a method for Migrator interface.
func (pg *Migrator) UnlockDB(ctx context.Context) error {
	_, err := pg.conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1);", lockNum)
	return err
}

// Version is a method for Migrator interface.
func (pg *Migrator) Version(ctx context.Context) (version int, err error) {
	query := "SELECT COUNT(*) FROM _dbump_schema_version;"
	row := pg.conn.QueryRowContext(ctx, query)
	err = row.Scan(&version)
	return version, err
}

// SetVersion is a method for Migrator interface.
func (pg *Migrator) SetVersion(ctx context.Context, version int) error {
	query := `INSERT INTO _dbump_schema_version (version, created_at)
VALUES ($1, NOW())
ON CONFLICT (version) DO UPDATE
SET created_at = NOW();`
	_, err := pg.conn.ExecContext(ctx, query, version)
	return err
}

// Exec is a method for Migrator interface.
func (pg *Migrator) Exec(ctx context.Context, query string, args ...interface{}) error {
	tx, err := pg.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("exec query: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
