package dbump

import (
	"context"
)

var _ Migrator = &MockMigrator{}

type MockMigrator struct {
	InitFn     func(ctx context.Context) error
	LockDBFn   func(ctx context.Context) error
	UnlockDBFn func(ctx context.Context) error

	VersionFn    func(ctx context.Context) (version int, err error)
	SetVersionFn func(ctx context.Context, version int) error

	ExecFn func(ctx context.Context, query string, args ...interface{}) error
}

func (mm *MockMigrator) Init(ctx context.Context) error {
	return mm.InitFn(ctx)
}
func (mm *MockMigrator) LockDB(ctx context.Context) error {
	return mm.LockDBFn(ctx)
}
func (mm *MockMigrator) UnlockDB(ctx context.Context) error {
	return mm.UnlockDBFn(ctx)
}

func (mm *MockMigrator) Version(ctx context.Context) (version int, err error) {
	return mm.VersionFn(ctx)
}
func (mm *MockMigrator) SetVersion(ctx context.Context, version int) error {
	return mm.SetVersionFn(ctx, version)
}

func (mm *MockMigrator) Exec(ctx context.Context, query string, args ...interface{}) error {
	return mm.ExecFn(ctx, query, args...)
}
