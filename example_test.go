package dbump_test

import (
	"context"

	"github.com/cristalhq/dbump"
)

func ExampleMigrator() {
	ctx := context.Background()
	var m dbump.Migrator
	var l dbump.Loader

	err := dbump.Run(ctx, dbump.Config{
		Migrator: m,
		Loader:   l,
		Mode:     dbump.ModeApplyAll,
	})
	if err != nil {
		panic(err)
	}
}
