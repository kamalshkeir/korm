package korm

import (
	"context"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

var DB_TEST_NAME = "testdb"

func TestMain(m *testing.M) {
	//sqlitedriver.Use()
	err := New(SQLITE, DB_TEST_NAME)
	if err != nil {
		log.Fatal(err)
	}
	// run the tests
	exitCode := m.Run()
	// Cleanup
	err = Shutdown(DB_TEST_NAME)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(DB_TEST_NAME + ".sqlite")
	if err != nil {
		log.Fatal(err)
	}
	// Exit
	os.Exit(exitCode)
}

type User struct {
	Id        uint   `korm:"pk"`
	Uuid      string `korm:"size:40;iunique"`
	Email     string `korm:"size:100;iunique"`
	Password  string
	IsAdmin   bool
	CreatedAt time.Time `korm:"now"`
	UpdatedAt time.Time `korm:"update"`
}

type UserNotMigrated struct {
	Id        uint   `korm:"pk"`
	Uuid      string `korm:"size:40;iunique"`
	Email     string `korm:"size:100;iunique"`
	Password  string
	IsAdmin   bool
	CreatedAt time.Time `korm:"now"`
	UpdatedAt time.Time `korm:"update"`
}

func TestMigrate(t *testing.T) {
	err := AutoMigrate[User]("users")
	if err != nil {
		t.Error(err)
	}
}

func TestInsertNonMigrated(t *testing.T) {
	_, err := Model[UserNotMigrated]().Insert(&UserNotMigrated{
		Uuid:     GenerateUUID(),
		Email:    "user-will-not-work@example.com",
		Password: "dqdqd",
		IsAdmin:  true,
	})
	if err == nil {
		t.Error("TestInsertNonMigrated did not error for not migrated model")
	}
}

func TestInsert(t *testing.T) {
	for i := 0; i < 10; i++ {
		iString := strconv.Itoa(i)
		_, err := Model[User]().Insert(&User{
			Uuid:      GenerateUUID(),
			Email:     "user-" + iString + "@example.com",
			Password:  "dqdqd",
			IsAdmin:   true,
			CreatedAt: time.Now(),
		})
		if err != nil {
			t.Log(err)
		}
	}
}

func TestInsertM(t *testing.T) {
	for i := 10; i < 20; i++ {
		iString := strconv.Itoa(i)
		_, err := Table("users").Insert(map[string]any{
			"uuid":       GenerateUUID(),
			"email":      "user-" + iString + "@example.com",
			"password":   "dqdqd",
			"is_admin":   true,
			"created_at": time.Now(),
		})
		if err != nil {
			t.Log(err)
		}
	}
}

func TestGetAll(t *testing.T) {
	u, err := Model[User]().All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("len not 20")
	}
}

func TestGetAllM(t *testing.T) {
	u, err := Table("users").All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("len not 20")
	}
}

func TestGetOne(t *testing.T) {
	u, err := Model[User]().Where("id = ?", 1).One()
	if err != nil {
		t.Error(err)
	}
	if !u.IsAdmin || u.Email == "" || u.CreatedAt.IsZero() || u.Uuid == "" {
		t.Error("wrong data:", u)
	}
}

func TestGetOneM(t *testing.T) {
	u, err := Table("users").Where("id = ?", 1).One()
	if err != nil {
		t.Error(err)
	}
	if u["is_admin"] != int64(1) || u["email"] == "" || u["uuid"] == "" {
		t.Error("wrong data:", u["is_admin"] != int64(1), u["email"] == "", u["uuid"] == "")
	}
}

func TestGetOneWithDebug(t *testing.T) {
	u, err := Model[User]().Debug().Where("id = ?", 1).One()
	if err != nil {
		t.Error(err)
	}
	if !u.IsAdmin || u.Email == "" || u.CreatedAt.IsZero() || u.Uuid == "" {
		t.Error("wrong data:", u)
	}
}

func TestGetOneWithDebugM(t *testing.T) {
	u, err := Table("users").Debug().Where("id = ?", 1).One()
	if err != nil {
		t.Error(err)
	}
	if u["is_admin"] != int64(1) || u["email"] == "" || u["uuid"] == "" {
		t.Error("wrong data:", u["is_admin"] != int64(1), u["email"] == "", u["uuid"] == "")
	}
}

func TestOrderBy(t *testing.T) {
	u, err := Model[User]().Where("is_admin = ?", true).OrderBy("-id").All()
	if err != nil {
		t.Error(err)
	}
	if u[0].Id != 20 || !u[0].IsAdmin || u[0].Email == "" || u[0].CreatedAt.IsZero() || u[0].Uuid == "" {
		t.Error("wrong data:", u[0])
	}
}

func TestOrderByM(t *testing.T) {
	u, err := Table("users").Where("is_admin = ?", true).OrderBy("-id").All()
	if err != nil {
		t.Error(err)
	}
	if u[0]["id"] != int64(20) || u[0]["is_admin"] != int64(1) || u[0]["email"] == "" || u[0]["uuid"] == "" {
		t.Error("wrong data:", u[0])
	}
}

func TestPagination(t *testing.T) {
	u, err := Model[User]().Where("is_admin = ?", true).Limit(5).Page(2).All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 5 || u[0].Id != 6 {
		t.Error("wrong data:", u[0])
	}
}

func TestPaginationM(t *testing.T) {
	u, err := Table("users").Where("is_admin = ?", true).Limit(5).Page(2).All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 5 || u[0]["id"] != int64(6) {
		t.Error("wrong data:", u[0])
	}
}

func TestWithCtx(t *testing.T) {
	u, err := Model[User]().Where("is_admin = ?", true).Context(context.Background()).All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("missing data")
	}
}

func TestWithCtxM(t *testing.T) {
	u, err := Table("users").Where("is_admin = ?", true).Context(context.Background()).All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("missing data")
	}
}

func TestQuery(t *testing.T) {
	u, err := Model[User]().Query("select * from users").All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("missing data")
	}
}

func TestQueryM(t *testing.T) {
	u, err := Table("users").Query("select * from users").All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("missing data")
	}
}

func TestSelect(t *testing.T) {
	u, err := Model[User]().Select("email").All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 || !u[0].CreatedAt.IsZero() || u[0].Email == "" || u[0].Password != "" {
		t.Error("wrong data:", u[0])
	}
}

func TestSelectM(t *testing.T) {
	u, err := Table("users").Select("email").All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 || len(u[0]) != 1 {
		t.Error("wrong data:", u[0])
	}
}

func TestBuilderStruct(t *testing.T) {
	u, err := BuilderStruct[User]().All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("wrong data len:", len(u))
	}
}

func TestBuilderMap(t *testing.T) {
	u, err := BuilderMap("users").All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("wrong data len:", len(u))
	}
}

func TestDatabase(t *testing.T) {
	u, err := Model[User]().Database(DB_TEST_NAME).All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("wrong data len:", len(u))
	}
}

func TestDatabaseM(t *testing.T) {
	u, err := Table("users").Database(DB_TEST_NAME).All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("wrong data len:", len(u))
	}
}

func TestUpdateSet(t *testing.T) {
	updatedEmail := "updated@example.com"
	n, err := Model[User]().Where("id = ?", 3).Set("email = ?", updatedEmail)
	if err != nil {
		t.Error(err)
	}
	if n <= 0 {
		t.Error("nothing updated, it should")
	}
	u, err := Model[User]().Where("id = ?", 3).One()
	if err != nil {
		t.Error(err)
	}
	if u.Email != updatedEmail {
		t.Errorf("expect %s got %s", updatedEmail, u.Email)
	}
}

func TestUpdateSetM(t *testing.T) {
	updatedEmail := "updated2@example.com"
	n, err := Table("users").Where("id = ?", 7).Set("email = ?", updatedEmail)
	if err != nil {
		t.Error(err)
	}
	if n <= 0 {
		t.Error("nothing updated, it should")
	}
	u, err := Model[User]().Where("id = ?", 7).One()
	if err != nil {
		t.Error(err)
	}
	if u.Email != updatedEmail {
		t.Errorf("expect %s got %s", updatedEmail, u.Email)
	}
}

func TestDelete(t *testing.T) {
	n, err := Model[User]().Where("id = ?", 12).Delete()
	if err != nil {
		t.Error(err)
	}
	if n <= 0 {
		t.Error("nothing deleted, it should")
	}
	_, err = Model[User]().Where("id = ?", 12).One()
	if err == nil {
		t.Error("not errored, it should")
	}
}

func TestDeleteM(t *testing.T) {
	n, err := Table("users").Where("id = ?", 13).Delete()
	if err != nil {
		t.Error(err)
	}
	if n <= 0 {
		t.Error("nothing deleted, it should")
	}
	_, err = Table("users").Where("id = ?", 12).One()
	if err == nil {
		t.Error("not errored, it should")
	}
}

func TestDrop(t *testing.T) {
	n, err := Model[User]().Drop()
	if err != nil {
		t.Error(err)
	}
	if n <= 0 {
		t.Error("nothing droped, it should")
	}
	for _, table := range GetAllTables() {
		if table == "users" {
			t.Error("users table not dropped", GetAllTables())
		}
	}
}
