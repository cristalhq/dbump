package dbump

import (
	"context"
	"database/sql"
)

// MigratorPostgres to migrate Postgres.
type MigratorPostgres struct {
	db           *sql.DB
	versionTable string
}

// NewMigratorPostgres instantiates new MigratorPostgres.
func NewMigratorPostgres(db *sql.DB) *MigratorPostgres {
	return &MigratorPostgres{
		db:           db,
		versionTable: "_schema_version",
	}
}

// LockDB is a method for Migrator interface.
func (pg *MigratorPostgres) LockDB(ctx context.Context) error {
	_, err := pg.db.ExecContext(ctx, "SELECT pg_advisory_lock($1)", lockNum)
	return err
}

// UnlockDB is a method for Migrator interface.
func (pg *MigratorPostgres) UnlockDB(ctx context.Context) error {
	_, err := pg.db.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockNum)
	return err
}

// Version is a method for Migrator interface.
func (pg *MigratorPostgres) Version(ctx context.Context) (version int, err error) {
	row := pg.db.QueryRowContext(ctx, "SELECT version FROM "+pg.versionTable)
	err = row.Scan(&version)
	return version, err
}

// SetVersion is a method for Migrator interface.
func (pg *MigratorPostgres) SetVersion(ctx context.Context, version int) error {
	_, err := pg.db.ExecContext(ctx, "UPDATE "+pg.versionTable+" SET version = $1", version)
	return err
}

// Exec is a method for Migrator interface.
func (pg *MigratorPostgres) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := pg.db.ExecContext(ctx, query)
	return err
}
