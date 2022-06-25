package dbump

import (
	"context"
	"fmt"
	"strconv"
)

var _ Migrator = &MockMigrator{}

type MockMigrator struct {
	log []string

	InitFn     func(ctx context.Context) error
	LockDBFn   func(ctx context.Context) error
	UnlockDBFn func(ctx context.Context) error

	VersionFn    func(ctx context.Context) (version int, err error)
	SetVersionFn func(ctx context.Context, version int) error

	BeginFn    func(ctx context.Context) error
	CommitFn   func(ctx context.Context) error
	RollbackFn func(ctx context.Context) error
	ExecFn     func(ctx context.Context, query string, args ...interface{}) error
}

func (mm *MockMigrator) Init(ctx context.Context) error {
	mm.log = append(mm.log, "init")
	if mm.InitFn == nil {
		return nil
	}
	return mm.InitFn(ctx)
}

func (mm *MockMigrator) LockDB(ctx context.Context) error {
	mm.log = append(mm.log, "lockdb")
	if mm.LockDBFn == nil {
		return nil
	}
	return mm.LockDBFn(ctx)
}

func (mm *MockMigrator) UnlockDB(ctx context.Context) error {
	mm.log = append(mm.log, "unlockdb")
	if mm.UnlockDBFn == nil {
		return nil
	}
	return mm.UnlockDBFn(ctx)
}

func (mm *MockMigrator) Version(ctx context.Context) (version int, err error) {
	mm.log = append(mm.log, "getversion")
	if mm.VersionFn == nil {
		return 0, nil
	}
	return mm.VersionFn(ctx)
}

func (mm *MockMigrator) SetVersion(ctx context.Context, version int) error {
	mm.log = append(mm.log, "setversion", strconv.Itoa(version))
	if mm.SetVersionFn == nil {
		return nil
	}
	return mm.SetVersionFn(ctx, version)
}

func (mm *MockMigrator) Begin(ctx context.Context) error {
	mm.log = append(mm.log, "begin")
	if mm.BeginFn == nil {
		return nil
	}
	return mm.BeginFn(ctx)
}
func (mm *MockMigrator) Commit(ctx context.Context) error {
	mm.log = append(mm.log, "commit")
	if mm.CommitFn == nil {
		return nil
	}
	return mm.CommitFn(ctx)
}
func (mm *MockMigrator) Rollback(ctx context.Context) error {
	mm.log = append(mm.log, "rollback")
	if mm.RollbackFn == nil {
		return nil
	}
	return mm.RollbackFn(ctx)
}

func (mm *MockMigrator) Exec(ctx context.Context, query string, args ...interface{}) error {
	mm.log = append(mm.log, "exec", query, fmt.Sprintf("%+v", args))
	if mm.ExecFn == nil {
		return nil
	}
	return mm.ExecFn(ctx, query, args...)
}

type MockLoader struct {
	LoaderFn func() ([]*Migration, error)
}

func (ml *MockLoader) Load() ([]*Migration, error) {
	return ml.LoaderFn()
}
