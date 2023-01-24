package benchmarks

import (
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/korm"
	//"gorm.io/driver/sqlite"
	//"gorm.io/gorm"
)

var DB_BENCH_NAME = "bench"
var NumberOfModel = 1000 // min 300

type TestTable struct {
	Id        uint `korm:"pk"`
	Email     string
	Content   string
	Password  string
	IsAdmin   bool
	CreatedAt time.Time `korm:"now"`
	UpdatedAt time.Time `korm:"update"`
}

type TestTableGorm struct {
	Id        uint `gorm:"primarykey"`
	Email     string
	Content   string
	Password  string
	IsAdmin   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

//var gormDB *gorm.DB

func TestMain(m *testing.M) {
	var err error
	//sqlitedriver.Use()
	err = korm.New(korm.SQLITE, DB_BENCH_NAME)
	if klog.CheckError(err) {
		return
	}
	// gormDB, err = gorm.Open(sqlite.Open("benchgorm.sqlite"), &gorm.Config{})
	// if klog.CheckError(err) {
	// 	return
	// }
	// migrate table test_table from struct TestTable
	err = korm.AutoMigrate[TestTable]("test_table")
	if klog.CheckError(err) {
		return
	}
	t, _ := korm.Table("test_table").All()
	if len(t) == 0 {
		for i := 0; i < NumberOfModel; i++ {
			_, err := korm.Model[TestTable]().Insert(&TestTable{
				Email:    "test-" + strconv.Itoa(i) + "@example.com",
				Content:  "Duis tortor odio, sodales quis lacinia quis, tincidunt id dolor. Curabitur tempor nunc at lacinia commodo. Aliquam sapien orci, rhoncus a cursus nec, accumsan ut tortor. Sed sed laoreet ipsum. Ut vulputate porttitor libero, non aliquet est rutrum nec. Nullam vitae viverra tortor.",
				Password: "aaffsbfaaaj2sbfsdjqbfsa2bfesfb",
				IsAdmin:  true,
			})
			klog.CheckError(err)
		}
	}
	// gorm
	// err = gormDB.AutoMigrate(&TestTableGorm{})
	// if klog.CheckError(err) {
	// 	return
	// }
	// dest := []TestTableGorm{}
	// err = gormDB.Find(&dest, &TestTableGorm{}).Error
	// if err != nil || len(dest) == 0 {
	// 	for i := 0; i < NumberOfModel; i++ {
	// 		err := gormDB.Create(&TestTableGorm{
	// 			Email:    "test-" + strconv.Itoa(i) + "@example.com",
	// 			Content:  "Duis tortor odio, sodales quis lacinia quis, tincidunt id dolor. Curabitur tempor nunc at lacinia commodo. Aliquam sapien orci, rhoncus a cursus nec, accumsan ut tortor. Sed sed laoreet ipsum. Ut vulputate porttitor libero, non aliquet est rutrum nec. Nullam vitae viverra tortor.",
	// 			Password: "aaffsbfaaaj2sbfsdjqbfsa2bfesfb",
	// 			IsAdmin:  true,
	// 		}).Error
	// 		if klog.CheckError(err) {
	// 			return
	// 		}
	// 	}
	// }

	//run tests
	exitCode := m.Run()

	err = korm.Shutdown(DB_BENCH_NAME)
	if klog.CheckError(err) {
		return
	}
	// gormdb, _ := gormDB.DB()
	// err = gormdb.Close()
	// if klog.CheckError(err) {
	// 	return
	// }
	// Cleanup for sqlite , remove file db
	err = os.Remove(DB_BENCH_NAME + ".sqlite")
	if err != nil {
		log.Fatal(err)
	}
	// err = os.Remove("benchgorm.sqlite")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	os.Exit(exitCode)
}

// func BenchmarkGetAllS_GORM(b *testing.B) {
// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		a := []TestTableGorm{}
// 		err := gormDB.Find(&a).Error
// 		if err != nil {
// 			b.Error("error BenchmarkGetAllS_GORM:", err)
// 		}
// 		if len(a) != NumberOfModel || a[0].Email != "test-0@example.com" {
// 			b.Error("Failed:", len(a), a[0].Email)
// 		}
// 	}
// }

func BenchmarkGetAllS(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a, err := korm.Model[TestTable]().All()
		if err != nil {
			b.Error("error BenchmarkGetAllS:", err)
		}
		if len(a) != NumberOfModel || a[0].Email != "test-0@example.com" {
			b.Error("Failed:", len(a), a[0].Email)
		}
	}
}

func BenchmarkQueryS(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a, err := korm.QueryS[TestTable]("", "select * from test_table where is_admin =?", true)
		if err != nil {
			b.Error("error BenchmarkQueryS:", err)
		}
		if len(a) != NumberOfModel || a[0].Email != "test-0@example.com" {
			b.Error("Failed:", len(a), a[0].Email)
		}
	}
}

func BenchmarkQueryM(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a, err := korm.Query("", "select * from test_table where is_admin =?", true)
		if err != nil {
			b.Error("error BenchmarkQueryM:", err)
		}
		if len(a) != NumberOfModel || a[0]["email"] != "test-0@example.com" {
			b.Error("Failed:", len(a), a[0]["email"])
		}
	}
}

// func BenchmarkGetAllM_GORM(b *testing.B) {
// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		a := []map[string]any{}
// 		err := gormDB.Table("test_table_gorms").Find(&a).Error
// 		if err != nil {
// 			b.Error("error BenchmarkGetAllM_GORM:", err)
// 		}
// 		if len(a) != NumberOfModel || a[0]["email"] != "test-0@example.com" {
// 			b.Error("Failed:", len(a), a[0]["email"])
// 		}
// 	}
// }

func BenchmarkGetAllM(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a, err := korm.Table("test_table").All()
		if err != nil {
			b.Error("error BenchmarkGetAllM:", err)
		}
		if len(a) != NumberOfModel || a[0]["email"] != "test-0@example.com" {
			b.Error("Failed:", len(a), a[0]["email"])
		}
	}
}

// func BenchmarkGetRowS_GORM(b *testing.B) {
// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		u := TestTableGorm{}
// 		err := gormDB.Where(&TestTableGorm{
// 			Email: "test-10@example.com",
// 		}).First(&u).Error
// 		if err != nil {
// 			b.Error("error BenchmarkGetRowS_GORM:", err)
// 		}
// 		if u.Email != "test-10@example.com" {
// 			b.Error("gorm failed BenchmarkGetRowS_GORM:", u)
// 		}
// 	}
// }

func BenchmarkGetRowS(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u, err := korm.Model[TestTable]().Where("email = ?", "test-10@example.com").One()
		if err != nil {
			b.Error("error BenchmarkGetRowS:", err)
		}
		if u.Email != "test-10@example.com" {
			b.Error("gorm failed BenchmarkGetRowS:", u)
		}
	}
}

// func BenchmarkGetRowM_GORM(b *testing.B) {
// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		u := map[string]any{}
// 		err := gormDB.Model(&TestTableGorm{}).Where(&TestTableGorm{
// 			Email: "test-10@example.com",
// 		}).First(&u).Error
// 		if err != nil {
// 			b.Error("error BenchmarkGetRowS_GORM:", err)
// 		}
// 		if u["email"] != "test-10@example.com" {
// 			b.Error("gorm failed BenchmarkGetRowM_GORM:", u)
// 		}
// 	}
// }

func BenchmarkGetRowM(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u, err := korm.Table("test_table").Where("email = ?", "test-10@example.com").One()
		if err != nil {
			b.Error("error BenchmarkGetRowM:", err)
		}
		if u["email"] != "test-10@example.com" {
			b.Error("gorm failed BenchmarkGetRowM:", u)
		}
	}
}

// func BenchmarkPagination10_GORM(b *testing.B) {
// 	page := 2
// 	pageSize := 10
// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		u := []TestTableGorm{}
// 		offset := (page - 1) * pageSize
// 		err := gormDB.Model(&TestTableGorm{}).Where(&TestTableGorm{
// 			IsAdmin: true,
// 		}).Offset(offset).Limit(pageSize).Find(&u).Error
// 		if err != nil {
// 			b.Error("error BenchmarkPagination10_GORM:", err)
// 		}
// 		if len(u) != pageSize || u[0].Email == "" {
// 			b.Error("error len BenchmarkPagination10_GORM:", len(u))
// 		}
// 	}
// }

func BenchmarkPagination10(b *testing.B) {
	page := 2
	pageSize := 10
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u, err := korm.Model[TestTable]().Where("is_admin", true).Page(page).Limit(pageSize).All()
		if err != nil {
			b.Error("error BenchmarkPagination10:", err)
		}
		if len(u) != pageSize || u[0].Email == "" {
			b.Error("error len BenchmarkPagination10:", len(u))
		}
	}
}

// func BenchmarkPagination100_GORM(b *testing.B) {
// 	page := 2
// 	pageSize := 100
// 	if NumberOfModel <= pageSize {
// 		return
// 	}
// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		u := []TestTableGorm{}
// 		offset := (page - 1) * pageSize
// 		err := gormDB.Model(&TestTableGorm{}).Where(&TestTableGorm{
// 			IsAdmin: true,
// 		}).Offset(offset).Limit(pageSize).Find(&u).Error
// 		if err != nil {
// 			b.Error("error BenchmarkPagination10_GORM:", err)
// 		}
// 		if len(u) != pageSize || u[0].Email == "" {
// 			b.Error("error len BenchmarkPagination10_GORM:", len(u))
// 		}
// 	}
// }

func BenchmarkPagination100(b *testing.B) {
	page := 2
	pageSize := 100
	if NumberOfModel <= pageSize {
		return
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u, err := korm.Model[TestTable]().Where("is_admin", true).Page(page).Limit(pageSize).All()
		if err != nil {
			b.Error("error BenchmarkPagination10:", err)
		}
		if len(u) != pageSize || u[0].Email == "" {
			b.Error("error len BenchmarkPagination10:", len(u))
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
