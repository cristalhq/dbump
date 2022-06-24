package dbump_pgx

import (
	"context"
	"fmt"
	"os"
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
		panic(err)
	}
}

func TestPGX_Simple(t *testing.T) {
	m := NewMigrator(conn)
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
		Mode:     dbump.ModeUp,
	})
	failIfErr(t, errRun)
}

func envOrDef(env, def string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return def
}

func failIfErr(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}
