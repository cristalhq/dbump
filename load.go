package dbump

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type FS interface {
	fs.FS
	fs.ReadDirFS
	fs.ReadFileFS
}

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

// FileSysLoader can load migrations from fs.FS.
type FileSysLoader struct {
	fsys FS
	path string
}

// NewFileSysLoader instantiates a new FileSysLoader.
func NewFileSysLoader(fsys FS, path string) *FileSysLoader {
	return &FileSysLoader{
		fsys: fsys,
		path: strings.TrimRight(path, string(os.PathSeparator)),
	}
}

// Load is a method for Loader interface.
func (el *FileSysLoader) Load() ([]*Migration, error) {
	return loadMigrationsFromFS(el.fsys, el.path)
}

// SliceLoader loads given migrations.
type SliceLoader struct {
	migrations []*Migration
}

// NewSliceLoader instantiates a new SliceLoader.
func NewSliceLoader(migrations []*Migration) *SliceLoader {
	return &SliceLoader{
		migrations: migrations,
	}
}

// Load is a method for Loader interface.
func (sl *SliceLoader) Load() ([]*Migration, error) {
	return sl.migrations, nil
}

// AddMigration to loader.
func (sl *SliceLoader) AddMigration(m *Migration) {
	if m == nil {
		panic("dbump: migration should not be nil")
	}
	sl.migrations = append(sl.migrations, m)
}

var migrationRE = regexp.MustCompile(`^(\d+)_.+\.sql$`)

func loadMigrationsFromFS(fsys FS, path string) ([]*Migration, error) {
	files, err := fsys.ReadDir(path)
	if err != nil {
		return nil, err
	}

	migs := make([]*Migration, 0, len(files))
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}

		matches := migrationRE.FindStringSubmatch(fi.Name())
		if len(matches) != 2 {
			continue
		}

		m, err := loadMigrationFromFS(fsys, path, matches[1], fi.Name())
		if err != nil {
			return nil, err
		}

		migs = append(migs, m)
	}
	return migs, nil
}

func loadMigrationFromFS(fsys FS, path, id, name string) (*Migration, error) {
	n, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		return nil, err
	}

	body, err := fsys.ReadFile(filepath.Join(path, name))
	if err != nil {
		return nil, err
	}

	m := parseMigration(body)
	m.ID = int(n)
	m.Name = name
	return m, nil
}

func parseMigration(body []byte) *Migration {
	// TODO(oleg): get name from magic comment
	parts := strings.SplitN(string(body), MigrationDelimiter, 2)
	applySQL := strings.TrimSpace(parts[0])

	var revertSQL string
	if len(parts) == 2 {
		revertSQL = strings.TrimSpace(parts[1])
	}

	return &Migration{
		Apply:  applySQL,
		Revert: revertSQL,
	}
}

type osFS struct{}

// Open implements dbump.FS interface.
func (osFS) Open(name string) (fs.File, error) {
	return os.Open(name)
}

// ReadDir implements dbump.FS interface.
func (osFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

// ReadFile implements dbump.FS interface.
func (osFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}
