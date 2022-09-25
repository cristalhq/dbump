package dbump_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cristalhq/dbump"
	"github.com/cristalhq/dbump/tests"
)

func TestRunCheck(t *testing.T) {
	testCases := []struct {
		testName string
		cfg      dbump.Config
	}{
		{
			testName: "migrator is nil",
			cfg:      dbump.Config{},
		},
		{
			testName: "loader is nil",
			cfg: dbump.Config{
				Migrator: &tests.MockMigrator{},
			},
		},
		{
			testName: "mode is ModeNotSet",
			cfg: dbump.Config{
				Migrator: &tests.MockMigrator{},
				Loader:   dbump.NewSliceLoader(nil),
			},
		},
		{
			testName: "bad mode",
			cfg: dbump.Config{
				Migrator: &tests.MockMigrator{},
				Loader:   dbump.NewSliceLoader(nil),
				Mode:     1000,
			},
		},
		{
			testName: "num not set",
			cfg: dbump.Config{
				Migrator: &tests.MockMigrator{},
				Loader:   dbump.NewSliceLoader(nil),
				Mode:     dbump.ModeApplyN,
			},
		},
		{
			testName: "num not set",
			cfg: dbump.Config{
				Migrator: &tests.MockMigrator{},
				Loader:   dbump.NewSliceLoader(nil),
				Mode:     dbump.ModeRevertN,
			},
		},
	}

	for _, tc := range testCases {
		failIfOk(t, dbump.Run(context.Background(), tc.cfg))
	}
}

func TestMigrate_ApplyAll(t *testing.T) {
	suite := tests.NewMigratorSuite(&tests.MockMigrator{})
	suite.ApplyAll(t)
}

func TestMigrate_ApplyOne(t *testing.T) {
	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 3, nil
		},
	}
	suite := tests.NewMigratorSuite(mm)

	suite.ApplyOne(t)
}

func TestMigrate_ApplyAllWhenFull(t *testing.T) {
	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 5, nil
		},
	}
	suite := tests.NewMigratorSuite(mm)

	suite.ApplyAllWhenFull(t)
}

func TestMigrate_RevertOne(t *testing.T) {
	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 3, nil
		},
	}
	suite := tests.NewMigratorSuite(mm)

	suite.RevertOne(t)
}

func TestMigrate_RevertAllWhenEmpty(t *testing.T) {
	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 0, nil
		},
	}
	suite := tests.NewMigratorSuite(mm)

	suite.RevertAllWhenEmpty(t)
}

func TestMigrate_RevertAll(t *testing.T) {
	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 5, nil
		},
	}
	suite := tests.NewMigratorSuite(mm)
	suite.RevertAll(t)
}

func TestMigrate_Redo(t *testing.T) {
	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 3, nil
		},
	}
	suite := tests.NewMigratorSuite(mm)
	suite.Redo(t)
}

func TestMigrate_Drop(t *testing.T) {
	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 5, nil
		},
	}
	suite := tests.NewMigratorSuite(mm)
	suite.Drop(t)
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

	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
		BeforeStep: func(ctx context.Context, step dbump.Step) {
			mm.LogAdd("before", fmt.Sprintf("{v:%d q:'%s' notx:%v}", step.Version, step.Query, step.DisableTx))
		},
		AfterStep: func(ctx context.Context, step dbump.Step) {
			mm.LogAdd("after", fmt.Sprintf("{v:%d q:'%s' notx:%v}", step.Version, step.Query, step.DisableTx))
		},
	}

	failIfErr(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestTimeout(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:false}",
		"unlockdb",
	}

	mm := &tests.MockMigrator{
		DoStepFn: func(ctx context.Context, step dbump.Step) error {
			select {
			case <-time.After(30 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
		Timeout:  20 * time.Millisecond,
	}

	failIfOk(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestDisableTx(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:true}",
		"unlockdb",
	}

	mm := &tests.MockMigrator{}
	cfg := dbump.Config{
		Migrator:  mm,
		Loader:    dbump.NewSliceLoader(testdataMigrations[:1]),
		Mode:      dbump.ModeApplyAll,
		DisableTx: true,
	}

	failIfErr(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
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

	mm := &tests.MockMigrator{}
	cfg := dbump.Config{
		Migrator:       mm,
		Loader:         dbump.NewSliceLoader(testdataMigrations),
		Mode:           dbump.ModeApplyAll,
		NoDatabaseLock: true,
	}

	failIfErr(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestUseForce(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"lockdb", "unlockdb", "lockdb", "init", "getversion",
		"dostep", "{v:4 q:'SELECT 4;' notx:false}",
		"dostep", "{v:5 q:'SELECT 5;' notx:false}",
		"unlockdb",
	}

	isLocked := true

	mm := &tests.MockMigrator{
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
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
		UseForce: true,
	}

	failIfErr(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestZigZag(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:false}",
		"dostep", "{v:0 q:'SELECT 10;' notx:false}",
		"dostep", "{v:1 q:'SELECT 1;' notx:false}",

		"dostep", "{v:2 q:'SELECT 2;' notx:false}",
		"dostep", "{v:1 q:'SELECT 20;' notx:false}",
		"dostep", "{v:2 q:'SELECT 2;' notx:false}",

		"dostep", "{v:3 q:'SELECT 3;' notx:false}",
		"dostep", "{v:2 q:'SELECT 30;' notx:false}",
		"dostep", "{v:3 q:'SELECT 3;' notx:false}",

		"dostep", "{v:4 q:'SELECT 4;' notx:false}",
		"dostep", "{v:3 q:'SELECT 40;' notx:false}",
		"dostep", "{v:4 q:'SELECT 4;' notx:false}",

		"dostep", "{v:5 q:'SELECT 5;' notx:false}",
		"dostep", "{v:4 q:'SELECT 50;' notx:false}",
		"dostep", "{v:5 q:'SELECT 5;' notx:false}",
		"unlockdb",
	}

	mm := &tests.MockMigrator{}
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
		ZigZag:   true,
	}

	failIfErr(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestFailOnInitError(t *testing.T) {
	wantLog := []string{"lockdb", "init", "unlockdb"}
	mm := &tests.MockMigrator{
		InitFn: func(ctx context.Context) error {
			return errors.New("no access")
		},
	}
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
	}

	failIfOk(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestFailOnLockDB(t *testing.T) {
	wantLog := []string{"lockdb"}
	mm := &tests.MockMigrator{
		LockDBFn: func(ctx context.Context) (err error) {
			return errors.New("no access")
		},
	}
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
	}

	failIfOk(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestFailOnUnlockDB(t *testing.T) {
	currVersion := 4
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:5 q:'SELECT 5;' notx:false}",
		"unlockdb",
	}
	mm := &tests.MockMigrator{
		UnlockDBFn: func(ctx context.Context) (err error) {
			return errors.New("no access")
		},
		VersionFn: func(ctx context.Context) (version int, err error) {
			return currVersion, nil
		},
	}
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
	}

	failIfOk(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestFailOnGetVersionError(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion", "unlockdb",
	}
	mm := &tests.MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 0, errors.New("no access")
		},
	}
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
	}

	failIfOk(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestFailOnDoStepError(t *testing.T) {
	wantLog := []string{
		"lockdb", "init", "getversion",
		"dostep", "{v:1 q:'SELECT 1;' notx:false}",
		"unlockdb",
	}
	mm := &tests.MockMigrator{
		DoStepFn: func(ctx context.Context, step dbump.Step) error {
			return errors.New("no access")
		},
	}
	cfg := dbump.Config{
		Migrator: mm,
		Loader:   dbump.NewSliceLoader(testdataMigrations),
		Mode:     dbump.ModeApplyAll,
	}

	failIfOk(t, dbump.Run(context.Background(), cfg))
	mustEqual(t, mm.Log(), wantLog)
}

func TestFailOnLoad(t *testing.T) {
	cfg := dbump.Config{
		Migrator: &tests.MockMigrator{},
		Loader: &MockLoader{
			LoaderFn: func() ([]*dbump.Migration, error) {
				return nil, errors.New("forgot to commit")
			},
		},
		Mode: dbump.ModeApplyAll,
	}
	failIfOk(t, dbump.Run(context.Background(), cfg))
}

func TestLoad(t *testing.T) {
	testCases := []struct {
		testName   string
		migrations []*dbump.Migration
		wantErr    error
	}{
		{
			"fail (missing migration)",
			[]*dbump.Migration{
				{ID: 3},
				{ID: 1},
			},
			errors.New("load: missing migration number: 2 (have 3)"),
		},

		{
			"fail (duplicate id)",
			[]*dbump.Migration{
				{ID: 2, Name: "mig2"},
				{ID: 2, Name: "mig2fix"},
				{ID: 1},
			},
			errors.New("load: duplicate migration number: 2 (mig2fix)"),
		},
	}

	for _, tc := range testCases {
		cfg := dbump.Config{
			Migrator: tests.NewMockMigrator(nil),
			Loader:   dbump.NewSliceLoader(tc.migrations),
			Mode:     dbump.ModeApplyAll,
		}

		err := dbump.Run(context.Background(), cfg)

		switch {
		case (err != nil) && (tc.wantErr != nil):
			mustEqual(t, err.Error(), tc.wantErr.Error())
		case err != nil:
			failIfErr(t, err)
		default:
			t.Fatal("want error")
		}
	}
}

type MockLoader struct {
	LoaderFn func() ([]*dbump.Migration, error)
}

func (ml *MockLoader) Load() ([]*dbump.Migration, error) {
	return ml.LoaderFn()
}

var testdataMigrations = []*dbump.Migration{
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
