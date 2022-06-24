# Guide for dbump

## Database support

To make maintainers & users life easier `dbump` keeps database-specific migrators in submobules.
This makes `go.mod` clean or even empty and simplifies security audit.

To find what database are supported see `dbump_*` directories in root:
* ClickHouse (TODO: link)
* MySQL (TODO: link)
* Postgres (TODO: link for pg & pgx)

## ZigZag mode

This mode is made to heavily test uses migrations but doing `apply-rollback-apply` of each migration (assuming going up).
In a such way every migration is truly verified and minimizes potential problems in a real world.

Detailed example, assuming we start with empty database (version 0):
1. Apply migration 1
2. Rollback migration 1
3. Apply migration 1
4. Apply migration 2
5. Rollback migration 2
6. Apply migration 2
...

Steps 1 is obvious but doing 2,3 verifies that migration can clean after itself and can be applied again.

Credits goes to [Postgres.ai](https://postgres.ai/) mentioning this feature at conference.

