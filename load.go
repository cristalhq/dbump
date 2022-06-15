package dbump

import (
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var migrationRE = regexp.MustCompile(`^(\d+)_.+\.sql$`)

func loadMigrationsFromFS(fs FS, path string) ([]*Migration, error) {
	files, err := fs.ReadDir(path)
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

		m, err := loadMigrationFromFS(fs, path, matches[1], fi.Name())
		if err != nil {
			return nil, err
		}

		migs = append(migs, m)
	}
	return migs, nil
}

func loadMigrationFromFS(fs FS, path, id, name string) (*Migration, error) {
	n, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		return nil, err
	}

	body, err := fs.ReadFile(filepath.Join(path, name))
	if err != nil {
		return nil, err
	}

	m := parseMigration(body)
	m.ID = int(n)
	m.Name = name
	return m, nil
}

func parseMigration(body []byte) *Migration {
	parts := strings.SplitN(string(body), MigrationDelimiter, 2)
	applySQL := strings.TrimSpace(parts[0])

	var rollbackSQL string
	if len(parts) == 2 {
		rollbackSQL = strings.TrimSpace(parts[1])
	}

	return &Migration{
		Apply:    applySQL,
		Rollback: rollbackSQL,
	}
}
