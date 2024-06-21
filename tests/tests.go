package tests

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cristalhq/dbump"
)

type MigratorSuite struct {
	migrator     dbump.Migrator
	ApplyTmpl    string
	RevertTmpl   string
	CleanMigTmpl string
	CleanTest    string
	SkipCleanup  bool
}

func NewMigratorSuite(m dbump.Migrator) *MigratorSuite {
	return &MigratorSuite{
		migrator: m,
		// some default harmless queries
		ApplyTmpl:  "SELECT %[2]d;",
		RevertTmpl: "SELECT %[2]d0;",
	}
}

func (suite *MigratorSuite) ApplyAll(t *testing.T) {
	migs := suite.genMigrations(t, 5, "apply_all")

	wantLog := []string{"lockdb", "init", "getversion"}
	for i, m := range migs {
		v := fmt.Sprintf(mockDoStepFmt, i+1, m.Apply, false)
		wantLog = append(wantLog, "dostep", v)
	}
	wantLog = append(wantLog, "unlockdb")

	mig := suite.getMockedMigrator()
	failIfErr(t, dbump.Run(context.Background(), dbump.Config{
		Migrator: mig,
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeApplyAll,
	}))
	mustEqual(t, mig.Log(), wantLog)
}

func (suite *MigratorSuite) ApplyOne(t *testing.T) {
	migs := suite.genMigrations(t, 5, "apply_one")
	curr := 3
	suite.prepare(t, migs[:curr])

	wantLog := []string{"lockdb", "init", "getversion"}
	v := fmt.Sprintf(mockDoStepFmt, 4, migs[3].Apply, false)
	wantLog = append(wantLog, "dostep", v)
	wantLog = append(wantLog, "unlockdb")

	mig := suite.getMockedMigrator()
	failIfErr(t, dbump.Run(context.Background(), dbump.Config{
		Migrator: mig,
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeApplyN,
		Num:      1,
	}))
	mustEqual(t, mig.Log(), wantLog)
}

func (suite *MigratorSuite) ApplyAllWhenFull(t *testing.T) {
	migs := suite.genMigrations(t, 5, "apply_all_when_full")
	suite.prepare(t, migs)

	wantLog := []string{"lockdb", "init", "getversion", "unlockdb"}

	mig := suite.getMockedMigrator()
	failIfErr(t, dbump.Run(context.Background(), dbump.Config{
		Migrator: mig,
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeApplyAll,
	}))
	mustEqual(t, mig.Log(), wantLog)
}

func (suite *MigratorSuite) RevertOne(t *testing.T) {
	migs := suite.genMigrations(t, 5, "revert_one")
	suite.prepare(t, migs[:3])

	wantLog := []string{"lockdb", "init", "getversion"}
	v := fmt.Sprintf(mockDoStepFmt, 2, migs[2].Revert, false)
	wantLog = append(wantLog, "dostep", v)
	wantLog = append(wantLog, "unlockdb")

	mig := suite.getMockedMigrator()
	failIfErr(t, dbump.Run(context.Background(), dbump.Config{
		Migrator: mig,
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeRevertN,
		Num:      1,
	}))
	mustEqual(t, mig.Log(), wantLog)
}

func (suite *MigratorSuite) RevertAllWhenEmpty(t *testing.T) {
	migs := suite.genMigrations(t, 5, "revert_all_when_empty")

	wantLog := []string{"lockdb", "init", "getversion", "unlockdb"}

	mig := suite.getMockedMigrator()
	failIfErr(t, dbump.Run(context.Background(), dbump.Config{
		Migrator: mig,
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeRevertAll,
	}))
	mustEqual(t, mig.Log(), wantLog)
}

func (suite *MigratorSuite) RevertAll(t *testing.T) {
	migs := suite.genMigrations(t, 5, "revert_all")
	suite.prepare(t, migs)

	wantLog := []string{"lockdb", "init", "getversion"}
	for _, m := range reverse(migs) {
		v := fmt.Sprintf(mockDoStepFmt, m.ID-1, m.Revert, false)
		wantLog = append(wantLog, "dostep", v)
	}
	wantLog = append(wantLog, "unlockdb")

	mig := suite.getMockedMigrator()
	failIfErr(t, dbump.Run(context.Background(), dbump.Config{
		Migrator: mig,
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeRevertAll,
	}))
	mustEqual(t, mig.Log(), wantLog)
}

func (suite *MigratorSuite) Redo(t *testing.T) {
	migs := suite.genMigrations(t, 5, "redo")
	suite.prepare(t, migs[:3])

	wantLog := []string{"lockdb", "init", "getversion"}
	{
		v := fmt.Sprintf(mockDoStepFmt, 2, migs[2].Revert, false)
		wantLog = append(wantLog, "dostep", v)
		v = fmt.Sprintf(mockDoStepFmt, 3, migs[2].Apply, false)
		wantLog = append(wantLog, "dostep", v)
	}
	wantLog = append(wantLog, "unlockdb")

	mig := suite.getMockedMigrator()
	failIfErr(t, dbump.Run(context.Background(), dbump.Config{
		Migrator: mig,
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeRedo,
	}))
	mustEqual(t, mig.Log(), wantLog)
}

func (suite *MigratorSuite) Drop(t *testing.T) {
	migs := suite.genMigrations(t, 5, "drop")
	suite.prepare(t, migs)

	wantLog := []string{"lockdb", "init", "getversion"}
	for i := 4; i >= 0; i-- {
		v := fmt.Sprintf(mockDoStepFmt, i, migs[i].Revert, false)
		wantLog = append(wantLog, "dostep", v)
	}
	wantLog = append(wantLog, "drop", "unlockdb")

	mig := suite.getMockedMigrator()
	failIfErr(t, dbump.Run(context.Background(), dbump.Config{
		Migrator: mig,
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeDrop,
	}))
	mustEqual(t, mig.Log(), wantLog)
}

// TODO:
// func TestTimeout(t *testing.T) {
// 	wantLog := []string{
// 		"lockdb", "init", "getversion",
// 		"dostep", "{v:1 q:'SELECT 1;' notx:false}",
// 		"unlockdb",
// 	}

// 	mm := &MockMigrator{
// 		DoStepFn: func(ctx context.Context, step dbump.Step) error {
// 			select {
// 			case <-time.After(30 * time.Second):
// 				return nil
// 			case <-ctx.Done():
// 				return ctx.Err()
// 			}
// 		},
// 	}
// 	cfg := dbump.Config{
// 		Migrator: mm,
// 		// Loader:   dbump.NewSliceLoader(testdataMigrations),
// 		Mode:    dbump.ModeApplyAll,
// 		Timeout: 20 * time.Millisecond,
// 	}

// 	failIfOk(t, dbump.Run(context.Background(), cfg))
// 	mustEqual(t, mm.log, wantLog)
// }

// apply given migrations, used only at the beginning of the test.
func (suite *MigratorSuite) prepare(tb testing.TB, migs []*dbump.Migration) {
	failIfErr(tb, dbump.Run(context.Background(), dbump.Config{
		Migrator: suite.getMockedMigrator(),
		Loader:   dbump.NewSliceLoader(migs),
		Mode:     dbump.ModeApplyAll,
	}))
}

func (suite *MigratorSuite) getMockedMigrator() *MockMigrator {
	// hack for dbump package tests, we do not need to wrap MockMigrator
	if mm, ok := suite.migrator.(*MockMigrator); ok {
		mm.log = []string{} // flush log for next assert
		return mm
	}
	return NewMockMigrator(suite.migrator)
}

func (suite *MigratorSuite) genMigrations(tb testing.TB, num int, testname string) []*dbump.Migration {
	res := make([]*dbump.Migration, 0, num)
	for i := 1; i <= num; i++ {
		res = append(res, &dbump.Migration{
			ID:     i,
			Name:   fmt.Sprintf("test-mig-%d", i),
			Apply:  fmt.Sprintf(suite.ApplyTmpl, testname, i),
			Revert: fmt.Sprintf(suite.RevertTmpl, testname, i),
		})
	}

	tb.Cleanup(func() {
		if suite.SkipCleanup {
			return
		}

		for i := 1; i <= num; i++ {
			query := fmt.Sprintf(suite.CleanMigTmpl, testname, i)
			failIfErr(tb, suite.migrator.DoStep(context.Background(), dbump.Step{
				Version: num - i - 1,
				Query:   query,
			}))
		}
		failIfErr(tb, suite.migrator.DoStep(context.Background(), dbump.Step{
			Query: suite.CleanTest,
		}))
	})
	return res
}

func reverse(migs []*dbump.Migration) []*dbump.Migration {
	res := make([]*dbump.Migration, len(migs))
	for i := range migs {
		res[i] = migs[len(migs)-i-1]
	}
	return res
}

func failIfOk(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fail()
	}
}

func failIfErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustEqual(t testing.TB, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("\nhave %+v\nwant %+v", got, want)
	}
}
