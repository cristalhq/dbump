package tests

import (
	"context"
	"fmt"

	"github.com/cristalhq/dbump"
)

const mockDoStepFmt = "{v:%d q:'%s' notx:%v}"

var _ dbump.Migrator = &MockMigrator{}

type MockMigrator struct {
	log []string

	LockDBFn   func(ctx context.Context) error
	UnlockDBFn func(ctx context.Context) error
	InitFn     func(ctx context.Context) error
	DropFn     func(ctx context.Context) error
	VersionFn  func(ctx context.Context) (version int, err error)
	DoStepFn   func(ctx context.Context, step dbump.Step) error
}

func NewMockMigrator(m dbump.Migrator) *MockMigrator {
	if m == nil {
		return &MockMigrator{}
	}
	return &MockMigrator{
		LockDBFn:   m.LockDB,
		UnlockDBFn: m.UnlockDB,
		InitFn:     m.Init,
		DropFn:     m.Drop,
		VersionFn:  m.Version,
		DoStepFn:   m.DoStep,
	}
}

func (mm *MockMigrator) Log() []string      { return mm.log }
func (mm *MockMigrator) LogAdd(s ...string) { mm.log = append(mm.log, s...) }

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

func (mm *MockMigrator) Drop(ctx context.Context) error {
	mm.log = append(mm.log, "drop")
	if mm.DropFn == nil {
		return nil
	}
	return mm.DropFn(ctx)
}

func (mm *MockMigrator) Version(ctx context.Context) (version int, err error) {
	mm.log = append(mm.log, "getversion")
	if mm.VersionFn == nil {
		return 0, nil
	}
	return mm.VersionFn(ctx)
}

func (mm *MockMigrator) DoStep(ctx context.Context, step dbump.Step) error {
	mm.log = append(mm.log, "dostep", fmt.Sprintf(mockDoStepFmt, step.Version, step.Query, step.DisableTx))
	if mm.DoStepFn == nil {
		return nil
	}
	return mm.DoStepFn(ctx, step)
}
