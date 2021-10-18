package dbump

import (
	"io/fs"
	"os"
	"regexp"
	"strings"
)

var migrationRE = regexp.MustCompile(`^(\d+)_.+\.sql$`)

// DiskLoader can load migrations from disk/OS.
type DiskLoader struct {
	path string
}

// NewDiskLoader instantiates a new DiskLoader.
func NewDiskLoader(path string) *DiskLoader {
	return &DiskLoader{
		path: strings.TrimRight(path, string(os.PathSeparator)),
	}
}

// Load is a method for Loader interface.
func (dl *DiskLoader) Load() ([]*Migration, error) {
	return loadMigrationsFromFS(osFS{}, dl.path)
}

type osFS struct{}

func (osFS) Open(name string) (fs.File, error) {
	panic("unreachable")
}

func (osFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (osFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}
