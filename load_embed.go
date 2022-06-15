package dbump

import (
	"io/fs"
	"os"
	"strings"
)

type FS interface {
	fs.FS
	fs.ReadDirFS
	fs.ReadFileFS
}

// FSLoader can load migrations from fs.FS.
type FSLoader struct {
	fs   FS
	path string
}

// NewFSLoader instantiates a new FSLoader.
func NewFSLoader(fs FS, path string) *FSLoader {
	return &FSLoader{
		fs:   fs,
		path: strings.TrimRight(path, string(os.PathSeparator)),
	}
}

// Load is a method for Loader interface.
func (el *FSLoader) Load() ([]*Migration, error) {
	return loadMigrationsFromFS(el.fs, el.path)
}
