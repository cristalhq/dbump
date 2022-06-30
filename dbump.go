package dbump

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ErrMigrationAlreadyLocked is returned only when migration lock is already hold.
// This might be in a situation when previous dbump migration has not finished properly
// or just someone already holds this lock. See Config.UseForce to force lock acquire.
var ErrMigrationAlreadyLocked = errors.New("migration is locked already")

// MigrationDelimiter separates apply and revert queries inside a migration step/file.
// Is exported just to be used by https://github.com/cristalhq/dbumper
const MigrationDelimiter = `--- apply above / revert below ---`

// Config for the migration process. Is used only by Run function.
type Config struct {
	// Migrator represents a database.
	Migrator Migrator

	// Loader of migrations.
	Loader Loader

	// Mode of the migration.
	// Default is zero ModeNotSet (zero value) which is an incorrect value.
	// Set mode explicitly to show how migration should be done.
	Mode MigratorMode

	// DisableTx will run every migration not in a transaction.
	// This completely depends on a specific Migrator implementation
	// because not every database supports transaction, so this option can be no-op all the time.
	DisableTx bool

	// UseForce to get a lock on a database. MUST be used with the caution.
	// Should be used when previous migration run didn't unlock the database,
	// and this blocks subsequent runs.
	UseForce bool

	// ZigZag migration. Useful in tests.
	// Going up does apply-revert-apply of each migration.
	// Going down does revert-apply-revert of each migration.
	ZigZag bool
}

// Migrator represents database over which we will run migrations.
type Migrator interface {
	// LockDB to prevent running other migrators at the same time.
	LockDB(ctx context.Context) error
	// UnlockDB to allow running other migrators later.
	UnlockDB(ctx context.Context) error

	// Init the dbump database where database state is saved.
	// What is created by this method completely depends on migrator implementation
	// and might be different between databases.
	Init(ctx context.Context) error

	// Version of the migration. Used only once in the beginning.
	Version(ctx context.Context) (version int, err error)

	// DoStep runs the given query and sets a new version on success.
	DoStep(ctx context.Context, step Step) error
}

// Step represents exact thing that is going to run.
type Step struct {
	Version   int
	Query     string
	DisableTx bool
}

// Loader returns migrations to be applied on a database.
type Loader interface {
	Load() ([]*Migration, error)
}

// Migration represents migration step that will be runned on a database.
type Migration struct {
	ID     int    // ID of the migration, unique, positive, starts from 1.
	Name   string // Name of the migration
	Apply  string // Apply query
	Revert string // Revert query
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

// AsLocklessMigrator makes given migrator to not take a lock on database.
func AsLocklessMigrator(m Migrator) Migrator {
	return &locklessMigrator{m}
}

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
	return m.runMigrations(ctx, migrations)
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
			// pass
		}
	}
	return ms, nil
}

func (m *mig) runMigrations(ctx context.Context, ms []*Migration) (err error) {
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

	defer func() {
		if errUnlock := m.UnlockDB(ctx); err == nil && errUnlock != nil {
			err = fmt.Errorf("unlock db: %w", errUnlock)
		}
	}()

	err = m.Init(ctx)
	if err != nil {
		return fmt.Errorf("init: %w", err)
	}
	err = m.runMigrationsLocked(ctx, ms)
	return err
}

func (m *mig) runMigrationsLocked(ctx context.Context, ms []*Migration) error {
	curr, target, err := m.getCurrAndTargetVersions(ctx, len(ms))
	if err != nil {
		return err
	}

	for _, step := range m.prepareSteps(curr, target, ms) {
		if err := m.DoStep(ctx, step); err != nil {
			return err
		}
	}
	return nil
}

func (m *mig) getCurrAndTargetVersions(ctx context.Context, migrations int) (curr, target int, err error) {
	curr, err = m.Version(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get version: %w", err)
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

func (m *mig) prepareSteps(curr, target int, ms []*Migration) []Step {
	if curr == target {
		return nil
	}
	steps := []Step{}

	direction := 1
	if curr > target {
		direction = -1
	}
	isUp := direction == 1

	for ; curr != target; curr += direction {
		idx := curr
		if !isUp {
			idx--
		}

		steps = append(steps, ms[idx].toStep(isUp, m.DisableTx))
		if m.ZigZag {
			steps = append(steps,
				ms[idx].toStep(!isUp, m.DisableTx),
				ms[idx].toStep(isUp, m.DisableTx))
		}
	}
	return steps
}

func (m *Migration) toStep(up, disableTx bool) Step {
	if up {
		return Step{
			Version:   m.ID,
			Query:     m.Apply,
			DisableTx: disableTx,
		}
	}
	return Step{
		Version:   m.ID - 1,
		Query:     m.Revert,
		DisableTx: disableTx,
	}
}

type locklessMigrator struct {
	m Migrator
}

func (llm *locklessMigrator) LockDB(ctx context.Context) error   { return nil }
func (llm *locklessMigrator) UnlockDB(ctx context.Context) error { return nil }

func (llm *locklessMigrator) Init(ctx context.Context) error { return llm.m.Init(ctx) }

func (llm *locklessMigrator) Version(ctx context.Context) (version int, err error) {
	return llm.m.Version(ctx)
}

func (llm *locklessMigrator) DoStep(ctx context.Context, step Step) error {
	return llm.m.DoStep(ctx, step)
}
