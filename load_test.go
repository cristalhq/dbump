package dbump_test

import (
	"embed"
	"testing"

	"github.com/cristalhq/dbump"
)

func TestDiskLoader(t *testing.T) {
	loader := dbump.NewDiskLoader("./testdata")
	migs, err := loader.Load()
	failIfErr(t, err)

	want := testdataMigrations
	mustEqual(t, len(migs), len(want))

	for i := range migs {
		mustEqual(t, migs[i], want[i])
	}
}

func TestDiskLoaderSubdir(t *testing.T) {
	loader := dbump.NewDiskLoader("./testdata/subdir")
	migs, err := loader.Load()
	failIfErr(t, err)

	want := testdataMigrations
	mustEqual(t, len(migs), len(want))

	for i := range migs {
		mustEqual(t, migs[i], want[i])
	}
}

//go:embed testdata
var testdata embed.FS

func TestEmbedLoader(t *testing.T) {
	loader := dbump.NewFileSysLoader(testdata, "testdata")
	migs, err := loader.Load()
	failIfErr(t, err)

	want := testdataMigrations
	mustEqual(t, len(migs), len(want))

	for i := range migs {
		mustEqual(t, migs[i], want[i])
	}
}

func TestEmbedLoaderSubdir(t *testing.T) {
	loader := dbump.NewFileSysLoader(testdata, "testdata/subdir")
	migs, err := loader.Load()
	failIfErr(t, err)

	want := testdataMigrations
	mustEqual(t, len(migs), len(want))

	for i := range migs {
		mustEqual(t, migs[i], want[i])
	}
}

func TestSliceLoader(t *testing.T) {
	size := len(testdataMigrations)
	loader := dbump.NewSliceLoader(testdataMigrations[:size-1])
	loader.AddMigration(testdataMigrations[size-1])

	migs, err := loader.Load()
	failIfErr(t, err)

	want := testdataMigrations
	mustEqual(t, len(migs), len(want))

	for i := range migs {
		mustEqual(t, migs[i], want[i])
	}
}

func TestBadFormat(t *testing.T) {
	loader := dbump.NewFileSysLoader(testdata, "testdata/bad")
	_, err := loader.Load()
	failIfOk(t, err)
}
