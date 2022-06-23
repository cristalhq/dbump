package dbump

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestMigrateUp(t *testing.T) {
	wantLog := []string{
		"init",
		"lockdb",
		"getversion",
		"exec", "SELECT 1;", "[]",
		"setversion", "1",
		"exec", "SELECT 2;", "[]",
		"setversion", "2",
		"exec", "SELECT 3;", "[]",
		"setversion", "3",
		"exec", "SELECT 4;", "[]",
		"setversion", "4",
		"exec", "SELECT 5;", "[]",
		"setversion", "5",
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
		"init",
		"lockdb",
		"getversion",
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
		Mode:     ModeUp,
	}

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
	mustEqual(t, mm.log, wantLog)
}

func TestMigrateDown(t *testing.T) {
	wantLog := []string{
		"init",
		"lockdb",
		"getversion",
		"exec", "SELECT 50;", "[]",
		"setversion", "4",
		"exec", "SELECT 40;", "[]",
		"setversion", "3",
		"exec", "SELECT 30;", "[]",
		"setversion", "2",
		"exec", "SELECT 20;", "[]",
		"setversion", "1",
		"exec", "SELECT 10;", "[]",
		"setversion", "0",
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
		"init",
		"lockdb",
		"getversion",
		"unlockdb",
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

func TestForce(t *testing.T) {
	wantLog := []string{
		"init",
		"lockdb",
		"unlockdb",
		"lockdb",
		"getversion",
		"exec", "SELECT 4;", "[]",
		"setversion", "4",
		"exec", "SELECT 5;", "[]",
		"setversion", "5",
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
			return 3, nil
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
					ApplyFn: func(ctx context.Context, db DB) error {
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

func TestZigZag(t *testing.T) {
	t.SkipNow()
	cfg := Config{
		ZigZag: true,
	}

	err := Run(context.Background(), cfg)
	failIfErr(t, err)
}

var testdataMigrations = []*Migration{
	{
		ID:       1,
		Name:     `0001_init.sql`,
		Apply:    `SELECT 1;`,
		Rollback: `SELECT 10;`,
		isQuery:  true,
	},
	{
		ID:       2,
		Name:     `0002_another.sql`,
		Apply:    `SELECT 2;`,
		Rollback: `SELECT 20;`,
		isQuery:  true,
	},
	{
		ID:       3,
		Name:     `0003_even-better.sql`,
		Apply:    `SELECT 3;`,
		Rollback: `SELECT 30;`,
		isQuery:  true,
	},
	{
		ID:       4,
		Name:     `0004_but_fix.sql`,
		Apply:    `SELECT 4;`,
		Rollback: `SELECT 40;`,
		isQuery:  true,
	},
	{
		ID:       5,
		Name:     `0005_final.sql`,
		Apply:    `SELECT 5;`,
		Rollback: `SELECT 50;`,
		isQuery:  true,
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
