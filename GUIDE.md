# Guide for dbump

## Database support

To make maintainers & users life easier `dbump` keeps database-specific migrators in submobules.
This makes `go.mod` clean or even empty and simplifies security audit.

To find what database are supported see `dbump_*` directories in root:
| Database | Go module  | Link  |
|---|---|---|
| ClickHouse     | `github.com/cristalhq/dbump/dbump_ch`      | [dbump_ch](https://github.com/cristalhq/dbump/tree/main/dbump_ch)
| Mongo          | `github.com/cristalhq/dbump/dbump_mongo`   | [dbump_mongo](https://github.com/cristalhq/dbump/tree/main/dbump_mongo)
| MySQL          | `github.com/cristalhq/dbump/dbump_mysql`   | [dbump_mysql](https://github.com/cristalhq/dbump/tree/main/dbump_mysql)
| MS SQL         | `github.com/cristalhq/dbump/dbump_mssql`   | [dbump_mssql](https://github.com/cristalhq/dbump/tree/main/dbump_mssql)
| Postgres       | `github.com/cristalhq/dbump/dbump_pg`      | [dbump_pg](https://github.com/cristalhq/dbump/tree/main/dbump_pg)
| Postgres (pgx) | `github.com/cristalhq/dbump/dbump_pgx`     | [dbump_pgx](https://github.com/cristalhq/dbump/tree/main/dbump_pgx)
| GCP Spanner    | `github.com/cristalhq/dbump/dbump_spanner` | [dbump_spanner](https://github.com/cristalhq/dbump/tree/main/dbump_spanner)

Just do: `go get github.com/cristalhq/dbump/dbump_XXX` with a specific `XXX` and you will be able to run migrations for this database.

## Migration modes

| Mode | Description |
|---|---|
| ModeNotSet  | Default value in config, should not be used.
| ModeUp      | Apply all the migrations that weren't applied yet.
| ModeDown    | Revert all the migrations that were applied.
| ModeUpOne   | Apply only 1 migration.
| ModeDownOne | Revert only current migration.
| ModeRedo    | Revert and apply again current migration.
| ModeDrop    | Revert all migrations and remove `dbump` table.

## ZigZag mode

This mode is made to heavily test uses migrations but doing `apply-revert-apply` of each migration (assuming going up).
In a such way every migration is truly verified and minimizes potential problems in a real world.

Detailed example, assuming we start with empty database (version 0):
1. Apply migration 1
2. Revert migration 1
3. Apply migration 1
4. Apply migration 2
5. Revert migration 2
6. Apply migration 2
...

Steps 1 is obvious but doing 2,3 verifies that migration can clean after itself and can be applied again.

Credits goes to [Postgres.ai](https://postgres.ai/) mentioning this feature at conference.

## Do not take database locks

If for some reason you don't want or you can't take lock on database (why???) there is `AsLocklessMigrator` function to achieve this:

```go
// let's take Postgres for example 
m := dbump_pg.NewMigrator(...)

// volia, now m is a migrator that will not take a lock
m = dbump.AsLocklessMigrator(m)

// pass m in config param as before
dbump.Run(...)
```

However, lock prevents from running few migrators at once, possible creating bad situations that's is hard to fix.

Also, not all migrators supports locks.

