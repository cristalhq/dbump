package dbump_pgx

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/cristalhq/dbump"
	"github.com/jackc/pgx/v4"
)

var conn *pgx.Conn

func init() {
	host := envOrDef("DBUMP_PG_HOST", "localhost")
	port := envOrDef("DBUMP_PG_PORT", "5432")
	username := envOrDef("DBUMP_PG_USER", "postgres")
	password := envOrDef("DBUMP_PG_PASS", "postgres")
	db := envOrDef("DBUMP_PG_DB", "postgres")
	sslmode := envOrDef("DBUMP_PG_SSL", "disable")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, username, password, db, sslmode)

	var err error
	conn, err = pgx.Connect(context.Background(), dsn)
	if err != nil {
		panic(fmt.Sprintf("dbump_pgx: cannot connect to container: %s", err))
	}
}

func TestNonDefaultSchemaTable(t *testing.T) {
	testCases := []struct {
		name          string
		schema        string
		table         string
		wantTableName string
		wantLockNum   int64
	}{
		{
			name:          "all empty",
			schema:        "",
			table:         "",
			wantTableName: "_dbump_log",
			wantLockNum:   -3987518601082986461,
		},
		{
			name:          "schema set",
			schema:        "test_schema",
			table:         "",
			wantTableName: "test_schema._dbump_log",
			wantLockNum:   1417388815471108263,
		},
		{
			name:          "table set",
			schema:        "",
			table:         "test_table",
			wantTableName: "test_table",
			wantLockNum:   8712390964734167792,
		},
		{
			name:          "schema and table set",
			schema:        "test_schema",
			table:         "test_table",
			wantTableName: "test_schema.test_table",
			wantLockNum:   4631047095544292572,
		},
	}

	for _, tc := range testCases {
		m := NewMigrator(conn, Config{
			Schema: tc.schema,
			Table:  tc.table,
		})
		mustEqual(t, m.cfg.tableName, tc.wantTableName)
		mustEqual(t, m.cfg.lockNum, tc.wantLockNum)
	}
}

func TestMigrateUp(t *testing.T) {
	migrations := []*dbump.Migration{
		{
			ID:     1,
			Apply:  "SELECT 1;",
			Revert: "SELECT 10;",
		},
		{
			ID:     2,
			Apply:  "SELECT 2;",
			Revert: "SELECT 20;",
		},
		{
			ID:     3,
			Apply:  "SELECT 3;",
			Revert: "SELECT 30;",
		},
	}

	cfg := dbump.Config{
		Migrator: NewMigrator(conn, Config{
			Schema: "TestSchemaUp",
			Table:  "TestMigrateUp",
		}),
		Loader: dbump.NewSliceLoader(migrations),
		Mode:   dbump.ModeApplyAll,
	}

	failIfErr(t, dbump.Run(context.Background(), cfg))
}

// func TestMigrateUpWhenFull(t *testing.T) {
// 	wantLog := []string{
// 		"init", "lockdb", "getversion", "unlockdb",
// 	}

// 	mm := &MockMigrator{
// 		VersionFn: func(ctx context.Context) (version int, err error) {
// 			return 5, nil
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 	}

// 	err := Run(context.Background(), cfg)
// 	failIfErr(t, err)
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestMigrateUpOne(t *testing.T) {
// 	currVersion := 3
// 	wantLog := []string{
// 		"init", "lockdb", "getversion",
// 		"exec", "SELECT 4;", "[]", "setversion", "4",
// 		"unlockdb",
// 	}

// 	mm := &MockMigrator{
// 		VersionFn: func(ctx context.Context) (version int, err error) {
// 			return currVersion, nil
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUpOne,
// 	}

// 	err := Run(context.Background(), cfg)
// 	failIfErr(t, err)
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestMigrateDown(t *testing.T) {
// 	wantLog := []string{
// 		"init", "lockdb", "getversion",
// 		"exec", "SELECT 50;", "[]", "setversion", "4",
// 		"exec", "SELECT 40;", "[]", "setversion", "3",
// 		"exec", "SELECT 30;", "[]", "setversion", "2",
// 		"exec", "SELECT 20;", "[]", "setversion", "1",
// 		"exec", "SELECT 10;", "[]", "setversion", "0",
// 		"unlockdb",
// 	}

// 	mm := &MockMigrator{
// 		VersionFn: func(ctx context.Context) (version int, err error) {
// 			return 5, nil
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeDown,
// 	}

// 	err := Run(context.Background(), cfg)
// 	failIfErr(t, err)
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestMigrateDownWhenEmpty(t *testing.T) {
// 	wantLog := []string{
// 		"init", "lockdb", "getversion", "unlockdb",
// 	}

// 	mm := &MockMigrator{
// 		VersionFn: func(ctx context.Context) (version int, err error) {
// 			return 0, nil
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeDown,
// 	}

// 	err := Run(context.Background(), cfg)
// 	failIfErr(t, err)
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestMigrateDownOne(t *testing.T) {
// 	currVersion := 3
// 	wantLog := []string{
// 		"init", "lockdb", "getversion",
// 		"exec", "SELECT 30;", "[]", "setversion", "2",
// 		"unlockdb",
// 	}

// 	mm := &MockMigrator{
// 		VersionFn: func(ctx context.Context) (version int, err error) {
// 			return currVersion, nil
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeDownOne,
// 	}

// 	err := Run(context.Background(), cfg)
// 	failIfErr(t, err)
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestUseForce(t *testing.T) {
// 	currVersion := 3
// 	wantLog := []string{
// 		"init", "lockdb", "unlockdb", "lockdb", "getversion",
// 		"exec", "SELECT 4;", "[]", "setversion", "4",
// 		"exec", "SELECT 5;", "[]", "setversion", "5",
// 		"unlockdb",
// 	}

// 	isLocked := true

// 	mm := &MockMigrator{
// 		LockDBFn: func(ctx context.Context) error {
// 			if isLocked {
// 				return errors.New("cannot get lock")
// 			}
// 			return nil
// 		},
// 		UnlockDBFn: func(ctx context.Context) error {
// 			isLocked = false
// 			return nil
// 		},
// 		VersionFn: func(ctx context.Context) (version int, err error) {
// 			return currVersion, nil
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 		UseForce: true,
// 	}

// 	err := Run(context.Background(), cfg)
// 	failIfErr(t, err)
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestZigZag(t *testing.T) {
// 	wantLog := []string{
// 		"init", "lockdb", "getversion",
// 		"exec", "SELECT 1;", "[]", "setversion", "1",
// 		"exec", "SELECT 10;", "[]", "setversion", "0",
// 		"exec", "SELECT 1;", "[]", "setversion", "1",

// 		"exec", "SELECT 2;", "[]", "setversion", "2",
// 		"exec", "SELECT 20;", "[]", "setversion", "1",
// 		"exec", "SELECT 2;", "[]", "setversion", "2",

// 		"exec", "SELECT 3;", "[]", "setversion", "3",
// 		"exec", "SELECT 30;", "[]", "setversion", "2",
// 		"exec", "SELECT 3;", "[]", "setversion", "3",

// 		"exec", "SELECT 4;", "[]", "setversion", "4",
// 		"exec", "SELECT 40;", "[]", "setversion", "3",
// 		"exec", "SELECT 4;", "[]", "setversion", "4",

// 		"exec", "SELECT 5;", "[]", "setversion", "5",
// 		"exec", "SELECT 50;", "[]", "setversion", "4",
// 		"exec", "SELECT 5;", "[]", "setversion", "5",
// 		"unlockdb",
// 	}

// 	mm := &MockMigrator{}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 		ZigZag:   true,
// 	}

// 	err := Run(context.Background(), cfg)
// 	failIfErr(t, err)
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestFailOnInitError(t *testing.T) {
// 	wantLog := []string{"init"}
// 	mm := &MockMigrator{
// 		InitFn: func(ctx context.Context) error {
// 			return errors.New("no access")
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 	}

// 	err := Run(context.Background(), cfg)
// 	if err == nil {
// 		t.Fail()
// 	}
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestFailOnLockDB(t *testing.T) {
// 	wantLog := []string{
// 		"init", "lockdb",
// 	}
// 	mm := &MockMigrator{
// 		LockDBFn: func(ctx context.Context) (err error) {
// 			return errors.New("no access")
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 	}

// 	err := Run(context.Background(), cfg)
// 	if err == nil {
// 		t.Fail()
// 	}
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestFailOnUnlockDB(t *testing.T) {
// 	currVersion := 4
// 	wantLog := []string{
// 		"init", "lockdb", "getversion",
// 		"exec", "SELECT 5;", "[]", "setversion", "5",
// 		"unlockdb",
// 	}
// 	mm := &MockMigrator{
// 		UnlockDBFn: func(ctx context.Context) (err error) {
// 			return errors.New("no access")
// 		},
// 		VersionFn: func(ctx context.Context) (version int, err error) {
// 			return currVersion, nil
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 	}

// 	err := Run(context.Background(), cfg)
// 	if err == nil {
// 		t.Fail()
// 	}
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestFailOnGetVersionError(t *testing.T) {
// 	wantLog := []string{
// 		"init", "lockdb", "getversion", "unlockdb",
// 	}
// 	mm := &MockMigrator{
// 		VersionFn: func(ctx context.Context) (version int, err error) {
// 			return 0, errors.New("no access")
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 	}

// 	err := Run(context.Background(), cfg)
// 	if err == nil {
// 		t.Fail()
// 	}
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestFailOnSetVersionError(t *testing.T) {
// 	wantLog := []string{
// 		"init", "lockdb", "getversion",
// 		"exec", "SELECT 1;", "[]", "setversion", "1",
// 		"unlockdb",
// 	}
// 	mm := &MockMigrator{
// 		SetVersionFn: func(ctx context.Context, version int) error {
// 			return errors.New("no access")
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 	}

// 	err := Run(context.Background(), cfg)
// 	if err == nil {
// 		t.Fail()
// 	}
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestFailOnExec(t *testing.T) {
// 	wantLog := []string{
// 		"init", "lockdb", "getversion",
// 		"exec", "SELECT 1;", "[]",
// 		"unlockdb",
// 	}
// 	mm := &MockMigrator{
// 		ExecFn: func(ctx context.Context, query string, args ...interface{}) error {
// 			return errors.New("syntax error")
// 		},
// 	}
// 	cfg := Config{
// 		Migrator: mm,
// 		Loader:   NewSliceLoader(testdataMigrations),
// 		Mode:     ModeUp,
// 	}

// 	err := Run(context.Background(), cfg)
// 	if err == nil {
// 		t.Fail()
// 	}
// 	mustEqual(t, mm.log, wantLog)
// }

// func TestFailOnLoad(t *testing.T) {
// 	cfg := Config{
// 		Migrator: &MockMigrator{},
// 		Loader: &MockLoader{
// 			LoaderFn: func() ([]*Migration, error) {
// 				return nil, errors.New("forgot to commit")
// 			},
// 		},
// 		Mode: ModeUp,
// 	}
// 	err := Run(context.Background(), cfg)
// 	if err == nil {
// 		t.Fail()
// 	}
// }

// func Test_loadMigrations(t *testing.T) {
// 	testCases := []struct {
// 		testName       string
// 		migrations     []*Migration
// 		wantMigrations []*Migration
// 		wantErr        error
// 	}{
// 		{
// 			"ok (migrations are sorted)",
// 			[]*Migration{
// 				{ID: 2},
// 				{ID: 1},
// 			},
// 			[]*Migration{
// 				{ID: 1},
// 				{ID: 2},
// 			},
// 			nil,
// 		},

// 		{
// 			"fail (missing migration)",
// 			[]*Migration{
// 				{ID: 3},
// 				{ID: 1},
// 			},
// 			nil,
// 			errors.New("missing migration number: 2 (have 3)"),
// 		},

// 		{
// 			"fail (duplicate id)",
// 			[]*Migration{
// 				{ID: 2, Name: "mig2"},
// 				{ID: 2, Name: "mig2fix"},
// 				{ID: 1},
// 			},
// 			nil,
// 			errors.New("duplicate migration number: 2 (mig2)"),
// 		},
// 	}

// 	for _, tc := range testCases {
// 		m := mig{
// 			Loader: NewSliceLoader(tc.migrations),
// 		}

// 		migs, err := m.load()
// 		mustEqual(t, err != nil, tc.wantErr != nil)
// 		mustEqual(t, migs, tc.wantMigrations)
// 	}
// }

// var testdataMigrations = []*Migration{
// 	{
// 		ID:     1,
// 		Name:   `0001_init.sql`,
// 		Apply:  `SELECT 1;`,
// 		Revert: `SELECT 10;`,
// 	},
// 	{
// 		ID:     2,
// 		Name:   `0002_another.sql`,
// 		Apply:  `SELECT 2;`,
// 		Revert: `SELECT 20;`,
// 	},
// 	{
// 		ID:     3,
// 		Name:   `0003_even-better.sql`,
// 		Apply:  `SELECT 3;`,
// 		Revert: `SELECT 30;`,
// 	},
// 	{
// 		ID:     4,
// 		Name:   `0004_but_fix.sql`,
// 		Apply:  `SELECT 4;`,
// 		Revert: `SELECT 40;`,
// 	},
// 	{
// 		ID:     5,
// 		Name:   `0005_final.sql`,
// 		Apply:  `SELECT 5;`,
// 		Revert: `SELECT 50;`,
// 	},
// }

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

func envOrDef(env, def string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return def
}
