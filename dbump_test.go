package dbump

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestRunCheck(t *testing.T) {
	testCases := []struct {
		testName string
		cfg      Config
	}{
		{
			testName: "migrator is nil",
			cfg:      Config{},
		},
		{
			testName: "loader is nil",
			cfg: Config{
				Migrator: &MockMigrator{},
			},
		},
		{
			testName: "mode is ModeNotSet",
			cfg: Config{
				Migrator: &MockMigrator{},
				Loader:   NewSliceLoader(nil),
			},
		},
		{
			testName: "bad mode",
			cfg: Config{
				Migrator: &MockMigrator{},
				Loader:   NewSliceLoader(nil),
				Mode:     modeMaxPossible + 1,
			},
		},
	}

	for _, tc := range testCases {
		failIfOk(t, Run(context.Background(), tc.cfg))
	}
}

func TestMigrateUp(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:true}",
		"dostep", "{v:2 q:'SELECT 2;' notx:true}",
		"dostep", "{v:3 q:'SELECT 3;' notx:true}",
		"dostep", "{v:4 q:'SELECT 4;' notx:true}",
		"dostep", "{v:5 q:'SELECT 5;' notx:true}",
		"unlockdb",
	}

	mm := &MockMigrator{}
	cfg := Config{
		Migrator:  mm,
		Loader:    NewSliceLoader(testdataMigrations),
		Mode:      ModeUp,
		DisableTx: true, // for shorter wantLog
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateUpWhenFull(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion", "unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 5, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateUpOne(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:4 q:'SELECT 4;' notx:false}",
		"unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUpOne,
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateDown(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:4 q:'SELECT 50;' notx:true}",
		"dostep", "{v:3 q:'SELECT 40;' notx:true}",
		"dostep", "{v:2 q:'SELECT 30;' notx:true}",
		"dostep", "{v:1 q:'SELECT 20;' notx:true}",
		"dostep", "{v:0 q:'SELECT 10;' notx:true}",
		"unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 5, nil
		},
	}
	cfg := Config{
		Migrator:  mm,
		Loader:    NewSliceLoader(testdataMigrations),
		Mode:      ModeDown,
		DisableTx: true, // for shorter wantLog
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateDownWhenEmpty(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion", "unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 0, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeDown,
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateDownOne(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:2 q:'SELECT 30;' notx:false}",
		"unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeDownOne,
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateRedo(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:2 q:'SELECT 30;' notx:false}",
		"dostep", "{v:3 q:'SELECT 3;' notx:false}",
		"unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeRedo,
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateDrop(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:2 q:'SELECT 30;' notx:false}",
		"dostep", "{v:1 q:'SELECT 20;' notx:false}",
		"dostep", "{v:0 q:'SELECT 10;' notx:false}",
		"drop",
		"unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeDrop,
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestBeforeAfterStep(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"lockdb", "init", "getversion",
		"before", "{v:4 q:'SELECT 4;' notx:false}",
		"dostep", "{v:4 q:'SELECT 4;' notx:false}",
		"after", "{v:4 q:'SELECT 4;' notx:false}",
		"before", "{v:5 q:'SELECT 5;' notx:false}",
		"dostep", "{v:5 q:'SELECT 5;' notx:false}",
		"after", "{v:5 q:'SELECT 5;' notx:false}",
		"unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
		BeforeStep: func(ctx context.Context, step Step) {
			mm.log = append(mm.log, "before", fmt.Sprintf("{v:%d q:'%s' notx:%v}", step.Version, step.Query, step.DisableTx))
		},
		AfterStep: func(ctx context.Context, step Step) {
			mm.log = append(mm.log, "after", fmt.Sprintf("{v:%d q:'%s' notx:%v}", step.Version, step.Query, step.DisableTx))
		},
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestTimeout(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:false}",
		"unlockdb",
	}

	mm := &MockMigrator{
		DoStepFn: func(ctx context.Context, step Step) error {
			select {
			case <-time.After(30 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
		Timeout:  20 * time.Millisecond,
	}

	failIfOk(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestLockless(t *testing.T) {
	wantLog := []string{
		"init",
		"getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:false}",
		"dostep", "{v:2 q:'SELECT 2;' notx:false}",
		"dostep", "{v:3 q:'SELECT 3;' notx:false}",
		"dostep", "{v:4 q:'SELECT 4;' notx:false}",
		"dostep", "{v:5 q:'SELECT 5;' notx:false}",
	}

	mm := &MockMigrator{}
	cfg := Config{
		Migrator: AsLocklessMigrator(mm),
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestUseForce(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"lockdb", "unlockdb", "lockdb", "init", "getversion",
		"dostep", "{v:4 q:'SELECT 4;' notx:true}",
		"dostep", "{v:5 q:'SELECT 5;' notx:true}",
		"unlockdb",
	}

	isLocked := true

	mm := &MockMigrator{
		LockDBFn: func(ctx context.Context) error {
			if isLocked {
				return errors.New("cannot get lock")
			}
			return nil
		},
		UnlockDBFn: func(ctx context.Context) error {
			isLocked = false
			return nil
		},
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := Config{
		Migrator:  mm,
		Loader:    NewSliceLoader(testdataMigrations),
		Mode:      ModeUp,
		UseForce:  true,
		DisableTx: true, // for shorter wantLog
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestZigZag(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:true}",
		"dostep", "{v:0 q:'SELECT 10;' notx:true}",
		"dostep", "{v:1 q:'SELECT 1;' notx:true}",

		"dostep", "{v:2 q:'SELECT 2;' notx:true}",
		"dostep", "{v:1 q:'SELECT 20;' notx:true}",
		"dostep", "{v:2 q:'SELECT 2;' notx:true}",

		"dostep", "{v:3 q:'SELECT 3;' notx:true}",
		"dostep", "{v:2 q:'SELECT 30;' notx:true}",
		"dostep", "{v:3 q:'SELECT 3;' notx:true}",

		"dostep", "{v:4 q:'SELECT 4;' notx:true}",
		"dostep", "{v:3 q:'SELECT 40;' notx:true}",
		"dostep", "{v:4 q:'SELECT 4;' notx:true}",

		"dostep", "{v:5 q:'SELECT 5;' notx:true}",
		"dostep", "{v:4 q:'SELECT 50;' notx:true}",
		"dostep", "{v:5 q:'SELECT 5;' notx:true}",
		"unlockdb",
	}

	mm := &MockMigrator{}
	cfg := Config{
		Migrator:  mm,
		Loader:    NewSliceLoader(testdataMigrations),
		Mode:      ModeUp,
		DisableTx: true, // for shorter wantLog
		ZigZag:    true,
	}

	failIfErr(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnInitError(t *testing.T) {
	wantLog := []string{"lockdb", "init", "unlockdb"}
	mm := &MockMigrator{
		InitFn: func(ctx context.Context) error {
			return errors.New("no access")
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	failIfOk(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnLockDB(t *testing.T) {
	wantLog := []string{"lockdb"}
	mm := &MockMigrator{
		LockDBFn: func(ctx context.Context) (err error) {
			return errors.New("no access")
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	failIfOk(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnUnlockDB(t *testing.T) {
	currVersion := 4
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:5 q:'SELECT 5;' notx:false}",
		"unlockdb",
	}
	mm := &MockMigrator{
		UnlockDBFn: func(ctx context.Context) (err error) {
			return errors.New("no access")
		},
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	failIfOk(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnGetVersionError(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion", "unlockdb",
	}
	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 0, errors.New("no access")
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	failIfOk(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnDoStepError(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:false}",
		"unlockdb",
	}
	mm := &MockMigrator{
		DoStepFn: func(ctx context.Context, step Step) error {
			return errors.New("no access")
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	failIfOk(t, Run(context.Background(), cfg))
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnLoad(t *testing.T) {
	cfg := Config{
		Migrator: &MockMigrator{},
		Loader: &MockLoader{
			LoaderFn: func() ([]*Migration, error) {
				return nil, errors.New("forgot to commit")
			},
		},
		Mode: ModeUp,
	}
	failIfOk(t, Run(context.Background(), cfg))
}

func Test_loadMigrations(t *testing.T) {
	testCases := []struct {
		testName       string
		migrations     []*Migration
		wantMigrations []*Migration
		wantErr        error
	}{
		{
			"ok (migrations are sorted)",
			[]*Migration{
				{ID: 2},
				{ID: 1},
			},
			[]*Migration{
				{ID: 1},
				{ID: 2},
			},
			nil,
		},

		{
			"fail (missing migration)",
			[]*Migration{
				{ID: 3},
				{ID: 1},
			},
			nil,
			errors.New("missing migration number: 2 (have 3)"),
		},

		{
			"fail (duplicate id)",
			[]*Migration{
				{ID: 2, Name: "mig2"},
				{ID: 2, Name: "mig2fix"},
				{ID: 1},
			},
			nil,
			errors.New("duplicate migration number: 2 (mig2)"),
		},
	}

	for _, tc := range testCases {
		m := mig{
			Loader: NewSliceLoader(tc.migrations),
		}

		migs, err := m.load()
		mustEqual(t, err != nil, tc.wantErr != nil)
		mustEqual(t, migs, tc.wantMigrations)
	}
}

var testdataMigrations = []*Migration{
	{
		ID:     1,
		Name:   `0001_init.sql`,
		Apply:  `SELECT 1;`,
		Revert: `SELECT 10;`,
	},
	{
		ID:     2,
		Name:   `0002_another.sql`,
		Apply:  `SELECT 2;`,
		Revert: `SELECT 20;`,
	},
	{
		ID:     3,
		Name:   `0003_even-better.sql`,
		Apply:  `SELECT 3;`,
		Revert: `SELECT 30;`,
	},
	{
		ID:     4,
		Name:   `0004_but_fix.sql`,
		Apply:  `SELECT 4;`,
		Revert: `SELECT 40;`,
	},
	{
		ID:     5,
		Name:   `0005_final.sql`,
		Apply:  `SELECT 5;`,
		Revert: `SELECT 50;`,
	},
}

func failIfOk(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fail()
	}
}

func failIfErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustEqual(t testing.TB, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("\nhave %+v\nwant %+v", got, want)
	}
}
