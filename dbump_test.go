package dbump

import (
	"context"
	"errors"
	"reflect"
	"testing"
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
		err := Run(context.Background(), tc.cfg)
		if err == nil {
			t.Fail()
		}
	}
}

func TestMigrateUp(t *testing.T) {
	wantLog := []string{
		"init", "lockdb", "getversion",
		"exec", "SELECT 1;", "[]", "setversion", "1",
		"exec", "SELECT 2;", "[]", "setversion", "2",
		"exec", "SELECT 3;", "[]", "setversion", "3",
		"exec", "SELECT 4;", "[]", "setversion", "4",
		"exec", "SELECT 5;", "[]", "setversion", "5",
		"unlockdb",
	}

	mm := &MockMigrator{}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateUpWhenFull(t *testing.T) {
	wantLog := []string{
		"init", "lockdb", "getversion", "unlockdb",
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

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateUpOne(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"init", "lockdb", "getversion",
		"exec", "SELECT 4;", "[]", "setversion", "4",
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

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateDown(t *testing.T) {
	wantLog := []string{
		"init", "lockdb", "getversion",
		"exec", "SELECT 50;", "[]", "setversion", "4",
		"exec", "SELECT 40;", "[]", "setversion", "3",
		"exec", "SELECT 30;", "[]", "setversion", "2",
		"exec", "SELECT 20;", "[]", "setversion", "1",
		"exec", "SELECT 10;", "[]", "setversion", "0",
		"unlockdb",
	}

	mm := &MockMigrator{
		VersionFn: func(ctx context.Context) (version int, err error) {
			return 5, nil
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeDown,
	}

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateDownWhenEmpty(t *testing.T) {
	wantLog := []string{
		"init", "lockdb", "getversion", "unlockdb",
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

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateDownOne(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"init", "lockdb", "getversion",
		"exec", "SELECT 30;", "[]", "setversion", "2",
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

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestUseForce(t *testing.T) {
	currVersion := 3
	wantLog := []string{
		"init", "lockdb", "unlockdb", "lockdb", "getversion",
		"exec", "SELECT 4;", "[]", "setversion", "4",
		"exec", "SELECT 5;", "[]", "setversion", "5",
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
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
		UseForce: true,
	}

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestZigZag(t *testing.T) {
	wantLog := []string{
		"init", "lockdb", "getversion",
		"exec", "SELECT 1;", "[]", "setversion", "1",
		"exec", "SELECT 10;", "[]", "setversion", "0",
		"exec", "SELECT 1;", "[]", "setversion", "1",

		"exec", "SELECT 2;", "[]", "setversion", "2",
		"exec", "SELECT 20;", "[]", "setversion", "1",
		"exec", "SELECT 2;", "[]", "setversion", "2",

		"exec", "SELECT 3;", "[]", "setversion", "3",
		"exec", "SELECT 30;", "[]", "setversion", "2",
		"exec", "SELECT 3;", "[]", "setversion", "3",

		"exec", "SELECT 4;", "[]", "setversion", "4",
		"exec", "SELECT 40;", "[]", "setversion", "3",
		"exec", "SELECT 4;", "[]", "setversion", "4",

		"exec", "SELECT 5;", "[]", "setversion", "5",
		"exec", "SELECT 50;", "[]", "setversion", "4",
		"exec", "SELECT 5;", "[]", "setversion", "5",
		"unlockdb",
	}

	mm := &MockMigrator{}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
		ZigZag:   true,
	}

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnInitError(t *testing.T) {
	wantLog := []string{"init"}
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

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fail()
	}
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnLockDB(t *testing.T) {
	wantLog := []string{
		"init", "lockdb",
	}
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

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fail()
	}
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnUnlockDB(t *testing.T) {
	currVersion := 4
	wantLog := []string{
		"init", "lockdb", "getversion",
		"exec", "SELECT 5;", "[]", "setversion", "5",
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

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fail()
	}
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnGetVersionError(t *testing.T) {
	wantLog := []string{
		"init", "lockdb", "getversion", "unlockdb",
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

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fail()
	}
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnSetVersionError(t *testing.T) {
	wantLog := []string{
		"init", "lockdb", "getversion",
		"exec", "SELECT 1;", "[]", "setversion", "1",
		"unlockdb",
	}
	mm := &MockMigrator{
		SetVersionFn: func(ctx context.Context, version int) error {
			return errors.New("no access")
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fail()
	}
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnExec(t *testing.T) {
	wantLog := []string{
		"init", "lockdb", "getversion",
		"exec", "SELECT 1;", "[]",
		"unlockdb",
	}
	mm := &MockMigrator{
		ExecFn: func(ctx context.Context, query string, args ...interface{}) error {
			return errors.New("syntax error")
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testdataMigrations),
		Mode:     ModeUp,
	}

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fail()
	}
	mustEqual(t, mm.log, wantLog)
}

func TestFailOnExecFunc(t *testing.T) {
	testMigrations := make([]*Migration, len(testdataMigrations))
	copy(testMigrations, testdataMigrations)

	testMigrations[0] = &Migration{
		ID:      1,
		isQuery: false,
		Apply:   "",
		Revert:  "",
		ApplyFn: func(ctx context.Context, conn Conn) error {
			return errors.New("nil dereference")
		},
		RevertFn: func(ctx context.Context, conn Conn) error {
			return errors.New("nil dereference")
		},
	}

	wantLog := []string{
		"init", "lockdb", "getversion", "unlockdb",
	}
	mm := &MockMigrator{
		ExecFn: func(ctx context.Context, query string, args ...interface{}) error {
			return errors.New("syntax error")
		},
	}
	cfg := Config{
		Migrator: mm,
		Loader:   NewSliceLoader(testMigrations),
		Mode:     ModeUp,
	}

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fail()
	}
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
	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fail()
	}
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
		{
			"fail (mix of query and func)",
			[]*Migration{
				{
					ID:    1,
					Apply: "do",
					ApplyFn: func(ctx context.Context, conn Conn) error {
						return nil
					},
				},
			},
			nil,
			errors.New("mixing queries and functions is not allowed (migration 1)"),
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
		ID:      1,
		Name:    `0001_init.sql`,
		Apply:   `SELECT 1;`,
		Revert:  `SELECT 10;`,
		isQuery: true,
	},
	{
		ID:      2,
		Name:    `0002_another.sql`,
		Apply:   `SELECT 2;`,
		Revert:  `SELECT 20;`,
		isQuery: true,
	},
	{
		ID:      3,
		Name:    `0003_even-better.sql`,
		Apply:   `SELECT 3;`,
		Revert:  `SELECT 30;`,
		isQuery: true,
	},
	{
		ID:      4,
		Name:    `0004_but_fix.sql`,
		Apply:   `SELECT 4;`,
		Revert:  `SELECT 40;`,
		isQuery: true,
	},
	{
		ID:      5,
		Name:    `0005_final.sql`,
		Apply:   `SELECT 5;`,
		Revert:  `SELECT 50;`,
		isQuery: true,
	},
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
