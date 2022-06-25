package dbump_mysql

import (
	"context"
	"database/sql"
	"fmt"
)

// to prevent multiple migrations running at the same time
const lockNum int64 = 777_777_777

// Migrator to migrate MySQL.
type Migrator struct {
	db           *sql.DB
	versionTable string
}

// NewMigrator instantiates new Migrator.
func NewMigrator(db *sql.DB) *Migrator {
	return &Migrator{
		db:           db,
		versionTable: "_dbump_schema_version",
	}
}

// Init migrator.
func (pg *Migrator) Init(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	version    BIGINT NOT NULL PRIMARY KEY,
	created_at TIMESTAMP NOT NULL
);`, pg.versionTable)
	_, err := pg.db.ExecContext(ctx, query)
	return err
}

// LockDB is a method for Migrator interface.
func (my *Migrator) LockDB(ctx context.Context) error {
	_, err := my.db.ExecContext(ctx, `SELECT GET_LOCK(?, 10)`, lockNum)
	return err
}

// UnlockDB is a method for Migrator interface.
func (my *Migrator) UnlockDB(ctx context.Context) error {
	_, err := my.db.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", lockNum)
	return err
}

// Version is a method for Migrator interface.
func (my *Migrator) Version(ctx context.Context) (version int, err error) {
	row := my.db.QueryRowContext(ctx, "SELECT version FROM "+my.versionTable)
	err = row.Scan(&version)
	return version, err
}

// SetVersion is a method for Migrator interface.
func (my *Migrator) SetVersion(ctx context.Context, version int) error {
	_, err := my.db.ExecContext(ctx, "UPDATE "+my.versionTable+" SET version = $1", version)
	return err
}

// Exec is a method for Migrator interface.
func (my *Migrator) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := my.db.ExecContext(ctx, query)
	return err
}
