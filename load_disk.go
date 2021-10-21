package dbump

import (
	"io/fs"
	"os"
	"strings"
)

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

// no-op just to implement dbump.fileSys interface.
func (osFS) Open(name string) (fs.File, error) {
	panic("unreachable")
}

// ReadDir implements dbump.fileSys interface.
func (osFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

// ReadFile implements dbump.fileSys interface.
func (osFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}
