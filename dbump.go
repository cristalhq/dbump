package dbump

import (
	"context"
	"fmt"
	"sort"
)

// MigrationDelimiter separates apply and rollback queries inside a migration step/file.
const MigrationDelimiter = `--- apply above / rollback below ---`

// to prevent multiple migrations running at the same time
const lockNum int64 = 777_777_777

// Migrator represents DB over which we will run migration queries.
type Migrator interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error

	Version(ctx context.Context) (version int, err error)
	SetVersion(ctx context.Context, version int) error

	Exec(ctx context.Context, query string, args ...interface{}) error
}

// Loader returns migrations to be applied on a DB.
type Loader interface {
	Load() ([]*Migration, error)
}

// Migration represents migration step that will be runned on DB.
type Migration struct {
	ID       int    // ID of the migration, unique, positive, starts from 1.
	Name     string // Name of the migration
	Apply    string // Apply query
	Rollback string // Rollback query
}

// Run the Migrator with migration queries provided by the Loader.
func Run(ctx context.Context, m Migrator, l Loader) error {
	ms, err := loadMigrations(l.Load())
	if err != nil {
		return err
	}
	return runMigration(ctx, m, ms)
}

func loadMigrations(ms []*Migration, err error) ([]*Migration, error) {
	if err != nil {
		return nil, err
	}

	sort.SliceStable(ms, func(i, j int) bool {
		return ms[i].ID < ms[j].ID
	})

	for i, m := range ms {
		switch want := i + 1; {
		case m.ID < want:
			return nil, fmt.Errorf("duplicate migration number: %d (%s)", m.ID, m.Name)
		case m.ID > want:
			return nil, fmt.Errorf("missing migration number: %d (have %d)", want, m.ID)
		default:
			// pass
		}
	}
	return ms, nil
}

func runMigration(ctx context.Context, m Migrator, ms []*Migration) error {
	if err := m.Lock(ctx); err != nil {
		return err
	}

	var err error
	defer func() {
		if errUnlock := m.Unlock(ctx); err == nil && errUnlock != nil {
			err = errUnlock
		}
	}()

	err = runLockedMigration(ctx, m, ms)
	return err
}

func runLockedMigration(ctx context.Context, m Migrator, ms []*Migration) error {
	currentVersion, err := m.Version(ctx)
	if err != nil {
		return err
	}

	// TODO: configure
	targetVersion := len(ms)
	switch {
	case targetVersion < 0 || len(ms) < targetVersion:
		fallthrough
	case currentVersion < 0 || len(ms) < currentVersion:
		return fmt.Errorf("target version %d is outside of range 0..%d ", targetVersion, len(ms))
	}

	direction := 1
	if currentVersion > targetVersion {
		direction = -1
	}

	for currentVersion != targetVersion {
		current := ms[currentVersion]
		sequence := current.ID
		query := current.Apply

		if direction == -1 {
			current = ms[currentVersion-1]
			sequence = current.ID - 1
			query = current.Rollback
		}

		if err := m.Exec(ctx, query); err != nil {
			return err
		}

		if err := m.SetVersion(ctx, sequence); err != nil {
			return err
		}
		currentVersion += direction
	}
	return nil
}
