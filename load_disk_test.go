package dbump

import (
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
