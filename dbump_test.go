package dbump

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestMigrate(t *testing.T) {
	log := []string{}
	wantLog := []string{
		"init",
		"lockdb",
		"getversion",
		"exec",
		"setversion",
		"exec",
		"setversion",
		"exec",
		"setversion",
		"unlockdb",
	}

	cfg := Config{
		Migrator: &MockMigrator{
			InitFn: func(ctx context.Context) error {
				log = append(log, "init")
				return nil
			},
			LockDBFn: func(ctx context.Context) error {
				log = append(log, "lockdb")
				return nil
			},
			UnlockDBFn: func(ctx context.Context) error {
				log = append(log, "unlockdb")
				return nil
			},

			VersionFn: func(ctx context.Context) (version int, err error) {
				log = append(log, "getversion")
				return 0, nil
			},
			SetVersionFn: func(ctx context.Context, version int) error {
				log = append(log, "setversion")
				return nil
			},

			ExecFn: func(ctx context.Context, query string, args ...interface{}) error {
				log = append(log, "exec")
				return nil
			},
		},
		Loader: NewSliceLoader(testdataMigrations),
		Mode:   ModeUp,
	}

	err := Run(context.Background(), cfg)
	failIfErr(t, err)

	if !reflect.DeepEqual(log, wantLog) {
		t.Fatalf("got %+v want %+v", log, wantLog)
	}
}

func TestLoadMigrations(t *testing.T) {
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
		if (err != nil) != (tc.wantErr != nil) {
			t.Fatalf("got %+v want %+v", err, tc.wantErr)
		}

		if !reflect.DeepEqual(migs, tc.wantMigrations) {
			t.Fatalf("got %+v want %+v", migs, tc.wantMigrations)
		}
	}
}

func TestZigZag(t *testing.T) {
	t.Skip()
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
}

func failIfErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
