package dbump

import (
	"context"
	"fmt"
)

var _ Migrator = &MockMigrator{}

type MockMigrator struct {
	log []string

	LockDBFn   func(ctx context.Context) error
	UnlockDBFn func(ctx context.Context) error
	InitFn     func(ctx context.Context) error
	VersionFn  func(ctx context.Context) (version int, err error)
	DoStepFn   func(ctx context.Context, step Step) error
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

func (mm *MockMigrator) Init(ctx context.Context) error {
	mm.log = append(mm.log, "init")
	if mm.InitFn == nil {
		return nil
	}
	return mm.InitFn(ctx)
}

func (mm *MockMigrator) Version(ctx context.Context) (version int, err error) {
	mm.log = append(mm.log, "getversion")
	if mm.VersionFn == nil {
		return 0, nil
	}
	return mm.VersionFn(ctx)
}

func (mm *MockMigrator) DoStep(ctx context.Context, step Step) error {
	mm.log = append(mm.log, "dostep", fmt.Sprintf("{v:%d q:'%s' notx:%v}", step.Version, step.Query, step.DisableTx))
	if mm.DoStepFn == nil {
		return nil
	}
	return mm.DoStepFn(ctx, step)
}

type MockLoader struct {
	LoaderFn func() ([]*Migration, error)
}

func (ml *MockLoader) Load() ([]*Migration, error) {
	return ml.LoaderFn()
}
