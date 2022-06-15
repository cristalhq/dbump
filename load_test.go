package dbump

import (
	"embed"
	"reflect"
	"testing"
)

func TestDiskLoader(t *testing.T) {
	loader := NewDiskLoader("./testdata")
	migs, err := loader.Load()
	if err != nil {
		t.Fatal(err)
	}

	want := testdataMigrations

	if len(migs) != len(want) {
		t.Fatalf("got %+v\nwant %+v", len(migs), len(want))
	}

	for i := range migs {
		if !reflect.DeepEqual(migs[i], want[i]) {
			t.Fatalf("got %+v\nwant %+v", migs[i], want[i])
		}
	}
}

func TestDiskLoaderSubdir(t *testing.T) {
	loader := NewDiskLoader("./testdata/subdir")
	migs, err := loader.Load()
	if err != nil {
		t.Fatal(err)
	}

	want := testdataMigrations

	if len(migs) != len(want) {
		t.Fatalf("got %+v\nwant %+v", len(migs), len(want))
	}

	for i := range migs {
		if !reflect.DeepEqual(migs[i], want[i]) {
			t.Fatalf("got %+v\nwant %+v", migs[i], want[i])
		}
	}
}

//go:embed testdata
var testdata embed.FS

func TestEmbedLoader(t *testing.T) {
	loader := NewFileSysLoader(testdata, "testdata")
	migs, err := loader.Load()
	if err != nil {
		t.Fatal(err)
	}

	want := testdataMigrations

	if len(migs) != len(want) {
		t.Fatalf("got %+v\nwant %+v", len(migs), len(want))
	}

	for i := range migs {
		if !reflect.DeepEqual(migs[i], want[i]) {
			t.Fatalf("got %+v\nwant %+v", migs[i], want[i])
		}
	}
}

func TestEmbedLoaderSubdir(t *testing.T) {
	loader := NewFileSysLoader(testdata, "testdata/subdir")
	migs, err := loader.Load()
	if err != nil {
		t.Fatal(err)
	}

	want := testdataMigrations

	if len(migs) != len(want) {
		t.Fatalf("got %+v\nwant %+v", len(migs), len(want))
	}

	for i := range migs {
		if !reflect.DeepEqual(migs[i], want[i]) {
			t.Fatalf("got %+v\nwant %+v", migs[i], want[i])
		}
	}
}

func TestSliceLoader(t *testing.T) {
	loader := NewSliceLoader(testdataMigrations[:2])
	loader.AddMigration(testdataMigrations[2])

	migs, err := loader.Load()
	if err != nil {
		t.Fatal(err)
	}

	want := testdataMigrations
	if len(migs) != len(want) {
		t.Fatalf("got %+v\nwant %+v", len(migs), len(want))
	}

	for i := range migs {
		if !reflect.DeepEqual(migs[i], want[i]) {
			t.Fatalf("got %+v\nwant %+v", migs[i], want[i])
		}
	}
}
