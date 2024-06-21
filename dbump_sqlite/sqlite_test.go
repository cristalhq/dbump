package dbump_sqlite

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"testing"

	"github.com/cristalhq/dbump"
	"github.com/cristalhq/dbump/tests"

	_ "github.com/mattn/go-sqlite3"
)

func TestSQLite_Simple(t *testing.T) {
	conn := newTestConn(t)

	m := NewMigrator(conn, Config{})
	l := dbump.NewSliceLoader([]*dbump.Migration{
		{
			ID:     1,
			Apply:  "SELECT 1;",
			Revert: "SELECT 1;",
		},
		{
			ID:     2,
			Apply:  "SELECT 1;",
			Revert: "SELECT 1;",
		},
		{
			ID:     3,
			Apply:  "SELECT 1;",
			Revert: "SELECT 1;",
		},
	})

	errRun := dbump.Run(context.Background(), dbump.Config{
		Migrator: m,
		Loader:   l,
		Mode:     dbump.ModeApplyAll,
	})
	mustOK(t, errRun)
}

func TestNonDefaultSchemaTable(t *testing.T) {
	conn := newTestConn(t)

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

func TestMigrate_ApplyAll(t *testing.T) {
	conn := newTestConn(t)
	newSuite(conn).ApplyAll(t)
}

func TestMigrate_ApplyOne(t *testing.T) {
	conn := newTestConn(t)
	newSuite(conn).ApplyOne(t)
}

func TestMigrate_ApplyAllWhenFull(t *testing.T) {
	conn := newTestConn(t)
	newSuite(conn).ApplyAllWhenFull(t)
}

func TestMigrate_RevertOne(t *testing.T) {
	conn := newTestConn(t)
	newSuite(conn).RevertOne(t)
}

func TestMigrate_RevertAllWhenEmpty(t *testing.T) {
	conn := newTestConn(t)
	newSuite(conn).RevertAllWhenEmpty(t)
}

func TestMigrate_RevertAll(t *testing.T) {
	conn := newTestConn(t)
	newSuite(conn).RevertAll(t)
}

func TestMigrate_Redo(t *testing.T) {
	conn := newTestConn(t)
	newSuite(conn).Redo(t)
}

func TestMigrate_Drop(t *testing.T) {
	conn := newTestConn(t)
	suite := newSuite(conn)
	suite.SkipCleanup = true
	suite.Drop(t)
}

var envPath = os.Getenv("DBUMP_SQLITE_PATH")

func newTestConn(tb testing.TB) *sql.DB {
	path := "./db.sqlitedb"
	if envPath != "" {
		path = envPath
	}

	// ignore error if it doesn't exist.
	_ = os.Remove(path)

	conn, err := sql.Open("sqlite3", path)
	mustOK(tb, err)

	tb.Cleanup(func() {
		_ = os.Remove(path)
	})
	return conn
}

func newSuite(conn *sql.DB) *tests.MigratorSuite {
	m := NewMigrator(conn, Config{})
	suite := tests.NewMigratorSuite(m)
	suite.ApplyTmpl = "CREATE TABLE %[1]s_%[2]d (id INT);"
	suite.RevertTmpl = "DROP TABLE %[1]s_%[2]d;"
	suite.CleanMigTmpl = "DROP TABLE IF EXISTS %[1]s_%[2]d;"
	suite.CleanTest = "DELETE FROM _dbump_log;"
	return suite
}

func mustOK(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}

func mustEqual(tb testing.TB, got, want interface{}) {
	tb.Helper()
	if !reflect.DeepEqual(got, want) {
		tb.Fatalf("\nhave %+v\nwant %+v", got, want)
	}
}
