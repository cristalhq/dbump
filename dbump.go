package dbump

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// MigrationDelimiter separates apply and rollback queries inside a migration step/file.
const MigrationDelimiter = `--- apply above / rollback below ---`

// Config for the migration process. Is used only by Run function.
type Config struct {
	// Migrator represents a database.
	Migrator Migrator

	// Loader of migrations.
	Loader Loader

	// UseForce to get a lock on a database.
	UseForce bool

	// Mode of the migration.
	// Default is zero (ModeNotSet) which is an incorrect value.
	// Set mode explicetly to show what should be done.
	Mode MigratorMode

	// ZigZag migration. Useful in tests.
	// Does apply-rollback-apply/rollback-apply-rollback of each migration.
	ZigZag bool
}

// Migrator represents database over which we will run migrations.
type Migrator interface {
	Init(ctx context.Context) error
	LockDB(ctx context.Context) error
	UnlockDB(ctx context.Context) error

	Version(ctx context.Context) (version int, err error)
	SetVersion(ctx context.Context, version int) error

	Exec(ctx context.Context, query string, args ...interface{}) error
}

// Loader returns migrations to be applied on a database.
type Loader interface {
	Load() ([]*Migration, error)
}

// Migration represents migration step that will be runned on a database.
type Migration struct {
	ID         int         // ID of the migration, unique, positive, starts from 1.
	Name       string      // Name of the migration
	Apply      string      // Apply query
	Rollback   string      // Rollback query
	ApplyFn    MigrationFn // Apply func
	RollbackFn MigrationFn // Rollback func

	isQuery bool // shortcut for the type of migration (query or func)
}

// MigrationFn ...
type MigrationFn func(ctx context.Context, db DB) error

// DB ...
type DB interface {
	Exec(ctx context.Context, query string, args ...interface{}) error
}

// MigratorMode to change migration flow.
type MigratorMode int

const (
	ModeNotSet MigratorMode = iota
	ModeUp
	ModeDown
	ModeUpOne
	ModeDownOne
	modeMaxPossible
)

// Run the Migrator with migration queries provided by the Loader.
func Run(ctx context.Context, config Config) error {
	switch {
	case config.Migrator == nil:
		return errors.New("migrator cannot be nil")
	case config.Loader == nil:
		return errors.New("loader cannot be nil")
	case config.Mode == ModeNotSet:
		return errors.New("mode not set")
	case config.Mode >= modeMaxPossible:
		return fmt.Errorf("incorrect mode provided: %d", config.Mode)
	}

	m := mig{
		Config:   config,
		Migrator: config.Migrator,
		Loader:   config.Loader,
	}
	return m.run(ctx)
}

type mig struct {
	Config
	Migrator
	Loader
}

func (m *mig) run(ctx context.Context) error {
	migrations, err := m.load()
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	return m.runMigration(ctx, migrations)
}

func (m *mig) load() ([]*Migration, error) {
	ms, err := m.Load()
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
			if (m.Apply != "" || m.Rollback != "") && (m.ApplyFn != nil || m.RollbackFn != nil) {
				return nil, fmt.Errorf("mixing queries and functions is not allowed (migration %d)", m.ID)
			}
			m.isQuery = m.Apply != ""
		}
	}
	return ms, nil
}

func (m *mig) runMigration(ctx context.Context, ms []*Migration) error {
	if err := m.Init(ctx); err != nil {
		return fmt.Errorf("init: %w", err)
	}
	if err := m.LockDB(ctx); err != nil {
		if !m.UseForce {
			return fmt.Errorf("lock db: %w", err)
		}
		if err := m.UnlockDB(ctx); err != nil {
			return fmt.Errorf("force unlock db: %w", err)
		}
		if err := m.LockDB(ctx); err != nil {
			return fmt.Errorf("force lock db: %w", err)
		}
	}

	var err error
	defer func() {
		if errUnlock := m.UnlockDB(ctx); err == nil && errUnlock != nil {
			err = fmt.Errorf("unlock db: %w", errUnlock)
		}
	}()

	err = m.runMigrationLocked(ctx, ms)
	return err
}

func (m *mig) runMigrationLocked(ctx context.Context, ms []*Migration) error {
	curr, target, err := m.getCurrAndTargetVersions(ctx, len(ms))
	if err != nil {
		return err
	}

	if curr == target {
		return nil
	}

	direction := 1
	if curr > target {
		direction = -1
	}

	// TODO(oleg): do ZigZag
	for curr != target {
		var current *Migration
		var sequence int
		var query string
		var queryFn MigrationFn

		switch {
		case direction == 1:
			current = ms[curr]
			sequence = current.ID
			query, queryFn = current.Apply, current.ApplyFn
		case direction == -1:
			current = ms[curr-1]
			sequence = current.ID - 1
			query, queryFn = current.Rollback, current.RollbackFn
		}

		if current.isQuery {
			err = m.Exec(ctx, query)
		} else {
			err = queryFn(ctx, m)
		}
		if err != nil {
			return fmt.Errorf("exec: %w", err)
		}

		if err := m.SetVersion(ctx, sequence); err != nil {
			return fmt.Errorf("set version: %w", err)
		}
		curr += direction
	}
	return nil
}

func (m *mig) getCurrAndTargetVersions(ctx context.Context, migrations int) (curr, target int, err error) {
	curr, err = m.Version(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get version: %w", err)
	}
	if curr < 0 {
		return 0, 0, fmt.Errorf("version is negative: %d (must be in range [0..)", curr)
	}

	switch m.Config.Mode {
	case ModeUp:
		target = migrations
		if curr > target {
			return 0, 0, errors.New("current is greater than target")
		}

	case ModeDown:
		if curr > migrations {
			return 0, 0, errors.New("current is greater than migrations count")
		}
		target = 0

	case ModeUpOne:
		target = curr + 1
		if target > migrations {
			return 0, 0, errors.New("target is greater than migrations count")
		}

	case ModeDownOne:
		if curr > migrations {
			return 0, 0, errors.New("current is greater than migrations count")
		}
		target = curr - 1

	default:
		panic("unreachable")
	}
	return curr, target, nil
}
