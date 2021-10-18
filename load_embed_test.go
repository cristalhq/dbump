package dbump

import (
	"embed"
	"reflect"
	"testing"
)

//go:embed testdata
var testdata embed.FS

func TestEmbedLoader(t *testing.T) {
	loader := NewEmbedLoader(testdata, "testdata")
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
	loader := NewEmbedLoader(testdata, "testdata/subdir")
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
