package benchmarks

import (
	"log"
	"os"
	"testing"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/korm"
)

var DB_BENCH_NAME = "bench"

type TestTable struct {
	Id      uint `korm:"pk"`
	Content string
}

func TestMain(m *testing.M) {
	//sqlitedriver.Use()
	korm.DisableCheck()
	var err error
	//sqlitedriver.Use()
	_ = korm.New(korm.SQLITE, DB_BENCH_NAME)
	// migrate table test_table from struct TestTable
	err = korm.AutoMigrate[TestTable]("test_table")
	if klog.CheckError(err) {
		return
	}
	t, _ := korm.Table("test_table").All()
	if len(t) == 0 {
		_, err := korm.Model[TestTable]().Insert(&TestTable{
			Content: "test",
		})
		klog.CheckError(err)
	}
	// run tests
	exitCode := m.Run()

	err = korm.Shutdown(DB_BENCH_NAME)
	if klog.CheckError(err) {
		return
	}
	// Cleanup for sqlite , remove file db
	err = os.Remove(DB_BENCH_NAME + ".sqlite")
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(exitCode)
}

func BenchmarkGetAllS(b *testing.B) {
	b.ReportAllocs()
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = korm.Model[TestTable]().All()
		if err != nil {
			b.Error("error BenchmarkGetAllS:", err)
		}
	}
}

func BenchmarkGetAllM(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := korm.Table("test_table").All()
		if err != nil {
			b.Error("error BenchmarkGetAllM:", err)
		}
	}
}

func BenchmarkGetRowS(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := korm.Model[TestTable]().Where("content = ?", "test").One()
		if err != nil {
			b.Error("error BenchmarkGetRowS:", err)
		}
	}
}

func BenchmarkGetRowM(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := korm.Table("test_table").Where("content = ?", "test").One()
		if err != nil {
			b.Error("error BenchmarkGetRowM:", err)
		}
	}
}

func BenchmarkGetAllTables(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := korm.GetAllTables()
		if len(t) == 0 {
			b.Error("error BenchmarkGetAllTables: no data")
		}
	}
}

func BenchmarkGetAllColumns(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := korm.GetAllColumnsTypes("test_table")
		if len(c) == 0 {
			b.Error("error BenchmarkGetAllColumns: no data")
		}
	}
}
