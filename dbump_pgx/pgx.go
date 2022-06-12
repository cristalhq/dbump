package dbump_pgx

import (
	"context"

	"github.com/cristalhq/dbump"
	"github.com/jackc/pgx/v4"
)

// to prevent multiple migrations running at the same time
const lockNum int64 = 777_777_777

var _ dbump.Migrator = &Migrator{}

// Migrator to migrate Postgres.
type Migrator struct {
	conn *pgx.Conn
}

// NewMigrator instantiates new Migrator.
func NewMigrator(conn *pgx.Conn) *Migrator {
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
	_, err := pg.conn.Exec(ctx, query)
	return err
}

// LockDB is a method for Migrator interface.
func (pg *Migrator) LockDB(ctx context.Context) error {
	_, err := pg.conn.Exec(ctx, "SELECT pg_advisory_lock($1);", lockNum)
	return err
}

// UnlockDB is a method for Migrator interface.
func (pg *Migrator) UnlockDB(ctx context.Context) error {
	_, err := pg.conn.Exec(ctx, "SELECT pg_advisory_unlock($1);", lockNum)
	return err
}

// Version is a method for Migrator interface.
func (pg *Migrator) Version(ctx context.Context) (version int, err error) {
	query := "SELECT COUNT(*) FROM _dbump_schema_version;"
	row := pg.conn.QueryRow(ctx, query)
	err = row.Scan(&version)
	return version, err
}

// SetVersion is a method for Migrator interface.
func (pg *Migrator) SetVersion(ctx context.Context, version int) error {
	query := `INSERT INTO _dbump_schema_version (version, created_at)
VALUES ($1, NOW())
ON CONFLICT (version) DO UPDATE
SET created_at = NOW();`
	_, err := pg.conn.Exec(ctx, query, version)
	return err
}

// Exec is a method for Migrator interface.
func (pg *Migrator) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := pg.conn.Exec(ctx, query, args...)
	return err
}
