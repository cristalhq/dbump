package dbump

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
		panic("migration should not be nil")
	}
	sl.migrations = append(sl.migrations, m)
}
