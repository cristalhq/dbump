# dbump

[![build-img]][build-url]
[![pkg-img]][pkg-url]
[![reportcard-img]][reportcard-url]
[![coverage-img]][coverage-url]
[![version-img]][version-url]

Go database schema migrator library (See [cristalhq/dbumper](https://github.com/cristalhq/dbumper) tool).

## Rationale

Most of the tools related to database migrations are bloated with the questionable features and dependecies. However, in most of the cases migrations are just files with queries to run. This library does this.

## Features

* Simple.
* Clean and tested code.
* Supports `fs.FS` and `go:embed`.
* `ZigZag` mode to better test migrations.
* Dependency-free (database connectors are optional).
* Supported databases:
  * Postgres
  * MySQL
  * ClickHouse
  * or your own!

See [GUIDE.md](https://github.com/cristalhq/dbump/blob/main/GUIDE.md) for more details.

## Install

Go version 1.16+

```
go get github.com/cristalhq/dbump
```

## Example

```go
ctx := context.Background()

cfg := dbump.Config{
	Migrator: dbump_pg.NewMigrator(db),
	Loader:   dbump.NewFileSysLoader(embed, "/"),
	Mode:     dbump.ModeUp,
}

err := dbump.Run(ctx, cfg)
if err != nil {
	panic(err)
}
```

Also see examples: [example_test.go](https://github.com/cristalhq/dbump/blob/main/example_test.go).

## Documentation

See [these docs][pkg-url].

## License

[MIT License](LICENSE).

[build-img]: https://github.com/cristalhq/dbump/workflows/build/badge.svg
[build-url]: https://github.com/cristalhq/dbump/actions
[pkg-img]: https://pkg.go.dev/badge/cristalhq/dbump
[pkg-url]: https://pkg.go.dev/github.com/cristalhq/dbump
[reportcard-img]: https://goreportcard.com/badge/cristalhq/dbump
[reportcard-url]: https://goreportcard.com/report/cristalhq/dbump
[coverage-img]: https://codecov.io/gh/cristalhq/dbump/branch/main/graph/badge.svg
[coverage-url]: https://codecov.io/gh/cristalhq/dbump
[version-img]: https://img.shields.io/github/v/release/cristalhq/dbump
[version-url]: https://github.com/cristalhq/dbump/releases
