package dbump

import (
	"errors"
	"reflect"
	"testing"
)

func TestLoadMigrations(t *testing.T) {
	_, err := loadMigrations(nil, errors.New("some error"))
	if err == nil {
		t.Fatal("expected err")
	}

	testCases := []struct {
		migrations     []*Migration
		wantMigrations []*Migration
		wantErr        error
	}{
		{
			[]*Migration{
				{ID: 2},
				{ID: 1},
			},
			[]*Migration{
				{ID: 1},
				{ID: 2},
			},
			nil,
		},

		{
			[]*Migration{
				{ID: 3},
				{ID: 1},
			},
			nil,
			errors.New("missing migration number: 2 (have 3)"),
		},

		{
			[]*Migration{
				{ID: 2, Name: "mig2"},
				{ID: 2, Name: "mig2fix"},
				{ID: 1},
			},
			nil,
			errors.New("duplicate migration number: 2 (mig2)"),
		},
	}

	for _, tc := range testCases {
		migs, err := loadMigrations(tc.migrations, nil)
		if (err != nil) != (tc.wantErr != nil) {
			t.Fatalf("got %+v want %+v", err, tc.wantErr)
		}

		if !reflect.DeepEqual(migs, tc.wantMigrations) {
			t.Fatalf("got %+v want %+v", migs, tc.wantMigrations)
		}
	}
}

var testdataMigrations = []*Migration{
	{
		ID:       1,
		Name:     `0001_init.sql`,
		Apply:    `SELECT 1;`,
		Rollback: `SELECT 10;`,
	},
	{
		ID:       2,
		Name:     `0002_another.sql`,
		Apply:    `SELECT 2;`,
		Rollback: `SELECT 20;`,
	},
	{
		ID:       3,
		Name:     `0003_even-better.sql`,
		Apply:    `SELECT 3;`,
		Rollback: `SELECT 30;`,
	},
}
