package dbump_pgx

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/cristalhq/dbump/tests"
	"github.com/jackc/pgx/v5"
)

var conn *pgx.Conn

func init() {
	var (
		host     = envOrDef("DBUMP_PG_HOST", "localhost")
		port     = envOrDef("DBUMP_PG_PORT", "5432")
		username = envOrDef("DBUMP_PG_USER", "postgres")
		password = envOrDef("DBUMP_PG_PASS", "postgres")
		db       = envOrDef("DBUMP_PG_DB", "postgres")
		sslmode  = envOrDef("DBUMP_PG_SSL", "disable")
	)

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
			wantTableName: "public._dbump_log",
			wantLockNum:   1542931740578198266,
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
			wantTableName: "public.test_table",
			wantLockNum:   8592189678091584965,
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
	newSuite().ApplyAll(t)
}

func TestMigrate_ApplyOne(t *testing.T) {
	newSuite().ApplyOne(t)
}

func TestMigrate_ApplyAllWhenFull(t *testing.T) {
	newSuite().ApplyAllWhenFull(t)
}

func TestMigrate_RevertOne(t *testing.T) {
	newSuite().RevertOne(t)
}

func TestMigrate_RevertAllWhenEmpty(t *testing.T) {
	newSuite().RevertAllWhenEmpty(t)
}

func TestMigrate_RevertAll(t *testing.T) {
	newSuite().RevertAll(t)
}

func TestMigrate_Redo(t *testing.T) {
	newSuite().Redo(t)
}

func TestMigrate_Drop(t *testing.T) {
	t.Skip()
	newSuite().Drop(t)
}

func newSuite() *tests.MigratorSuite {
	m := NewMigrator(conn, Config{})
	suite := tests.NewMigratorSuite(m)
	suite.ApplyTmpl = "CREATE TABLE public.%[1]s_%[2]d (id INT);"
	suite.RevertTmpl = "DROP TABLE public.%[1]s_%[2]d;"
	suite.CleanMigTmpl = "DROP TABLE IF EXISTS public.%[1]s_%[2]d;"
	suite.CleanTest = "TRUNCATE TABLE _dbump_log;"
	return suite
}

func mustEqual(tb testing.TB, got, want interface{}) {
	tb.Helper()
	if !reflect.DeepEqual(got, want) {
		tb.Fatalf("\nhave %+v\nwant %+v", got, want)
	}
}

func envOrDef(env, def string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return def
}
