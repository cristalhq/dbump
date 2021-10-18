package dbump

import (
	"embed"
	"os"
	"strings"
)

// EmbedLoader can load migrations from embed.FS.
type EmbedLoader struct {
	fs   embed.FS
	path string
}

// NewEmbedLoader instantiates a new EmbedLoader.
func NewEmbedLoader(fs embed.FS, path string) *EmbedLoader {
	return &EmbedLoader{
		fs:   fs,
		path: strings.TrimRight(path, string(os.PathSeparator)),
	}
}

// Load is a method for Loader interface.
func (el *EmbedLoader) Load() ([]*Migration, error) {
	return loadMigrationsFromFS(el.fs, el.path)
}
