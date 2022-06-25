package dbump_ch

import (
	"context"
	"database/sql"
)

// MigratorClickHouse to migrate ClickHouse.
type MigratorClickHouse struct {
	db           *sql.DB
	versionTable string
}

// NewMigratorClickHouse instantiates new MigratorClickHouse.
func NewMigratorClickHouse(db *sql.DB) *MigratorClickHouse {
	return &MigratorClickHouse{
		db:           db,
		versionTable: "_dbump_schema_version",
	}
}

// LockDB is a method for Migrator interface.
func (ch *MigratorClickHouse) LockDB(ctx context.Context) error {
	// TODO: currently no-op
	return nil
}

// UnlockDB is a method for Migrator interface.
func (ch *MigratorClickHouse) UnlockDB(ctx context.Context) error {
	// TODO: currently no-op
	return nil
}

// Version is a method for Migrator interface.
func (ch *MigratorClickHouse) Version(ctx context.Context) (version int, err error) {
	row := ch.db.QueryRowContext(ctx, "SELECT version FROM "+ch.versionTable)
	err = row.Scan(&version)
	return version, err
}

// SetVersion is a method for Migrator interface.
func (ch *MigratorClickHouse) SetVersion(ctx context.Context, version int) error {
	_, err := ch.db.ExecContext(ctx, "UPDATE "+ch.versionTable+" SET version = $1", version)
	return err
}

// Exec is a method for Migrator interface.
func (ch *MigratorClickHouse) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := ch.db.ExecContext(ctx, query)
	return err
}
