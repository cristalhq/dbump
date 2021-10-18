package dbump

import (
	"reflect"
	"testing"
)

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
