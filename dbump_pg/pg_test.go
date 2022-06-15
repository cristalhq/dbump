package dbump_pg_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/cristalhq/dbump"
	"github.com/cristalhq/dbump/dbump_pg"

	_ "github.com/lib/pq"
)

var sqldb *sql.DB

func init() {
	var (
		host     = os.Getenv("DBUMP_PG_HOST")
		port     = os.Getenv("DBUMP_PG_PORT")
		username = os.Getenv("DBUMP_PG_USER")
		password = os.Getenv("DBUMP_PG_PASS")
		db       = os.Getenv("DBUMP_PG_DB")
		sslmode  = os.Getenv("DBUMP_PG_SSL")
	)

	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5432"
	}
	if username == "" {
		username = "postgres"
	}
	if password == "" {
		password = "postgres"
	}
	if db == "" {
		db = "postgres"
	}
	if sslmode == "" {
		sslmode = "disable"
	}
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, username, password, db, sslmode)

	var err error
	sqldb, err = sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}
}

func TestPG_Simple(t *testing.T) {
	m := dbump_pg.NewMigrator(sqldb)
	l := dbump.NewSliceLoader([]*dbump.Migration{
		{
			ID:       1,
			Apply:    "SELECT 1;",
			Rollback: "SELECT 1;",
		},
		{
			ID:       2,
			Apply:    "SELECT 1;",
			Rollback: "SELECT 1;",
		},
		{
			ID:       3,
			Apply:    "SELECT 1;",
			Rollback: "SELECT 1;",
		},
	})

	errRun := dbump.Run(context.Background(), m, l)
	failIfErr(t, errRun)
}

func failIfErr(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}
