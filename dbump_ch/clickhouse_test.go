package dbump_ch

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go" // to register ClichHouse client
	"github.com/cristalhq/dbump"
)

var conn *sql.DB

func init() {
	host := envOrDef("DBUMP_CH_HOST", "localhost")
	port := envOrDef("DBUMP_CH_PORT", "9000")
	username := envOrDef("DBUMP_CH_USER", "clickhouse")
	password := envOrDef("DBUMP_CH_PASS", "clickhouse")
	database := envOrDef("DBUMP_CH_DB", "clickhouse")

	connURL := url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(username, password),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   database,
	}

	var err error
	conn, err = sql.Open("clickhouse", connURL.String())
	if err != nil {
		panic(fmt.Sprintf("dbump_ch: cannot connect to container: %s", err))
	}
}

func TestNonDefaultSchemaTable(t *testing.T) {
	testCases := []struct {
		name          string
		database      string
		table         string
		wantTableName string
	}{
		{
			name:          "all empty",
			database:      "",
			table:         "",
			wantTableName: "_dbump_log",
		},
		{
			name:          "schema set",
			database:      "test_schema",
			table:         "",
			wantTableName: "test_schema._dbump_log",
		},
		{
			name:          "table set",
			database:      "",
			table:         "test_table",
			wantTableName: "test_table",
		},
		{
			name:          "schema and table set",
			database:      "test_schema",
			table:         "test_table",
			wantTableName: "test_schema.test_table",
		},
	}

	for _, tc := range testCases {
		m := NewMigrator(conn, Config{
			Database: tc.database,
			Table:    tc.table,
		})
		mustEqual(t, m.cfg.tableName, tc.wantTableName)
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
			Table: "TestMigrateUp",
		}),
		Loader: dbump.NewSliceLoader(migrations),
		Mode:   dbump.ModeUp,
	}

	failIfErr(t, dbump.Run(context.Background(), cfg))
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

func envOrDef(env, def string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return def
}
