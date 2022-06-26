start-postgres:
	podman stop dbump-postgres
	podman rm dbump-postgres
	podman run --name dbump-postgres -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -d postgres

test-pgx:
	cd dbump_pgx && go test -v -race -shuffle=on ./...
