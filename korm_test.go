package korm

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

var DB_TEST_NAME = "test"

func TestMain(m *testing.M) {
	//sqlitedriver.Use()
	DisableCheck()
	err := New(SQLITE, DB_TEST_NAME)
	if err != nil {
		log.Fatal(err)
	}
	// run tests
	exitCode := m.Run()
	// Cleanup for sqlite , remove file db
	err = os.Remove(DB_TEST_NAME + ".sqlite")
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(exitCode)
}

type TestUser struct {
	Id        *uint   `korm:"pk"`
	Uuid      string  `korm:"size:40;iunique"`
	Email     *string `korm:"size:100;iunique"`
	Gen       string  `korm:"size:250;generated: concat(uuid,'working',len(password))"`
	Password  string
	Gen2      int `korm:"generated:len(password)*2"`
	IsAdmin   *bool
	CreatedAt time.Time `korm:"now"`
	UpdatedAt time.Time `korm:"update"`
}

type Group struct {
	Id   uint `korm:"pk"`
	Name string
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
	err := AutoMigrate[TestUser]("users")
	if err != nil {
		t.Error(err)
	}
	err = AutoMigrate[Group]("groups")
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

func TestGetAllTables(t *testing.T) {
	tables := GetAllTables(DB_TEST_NAME)
	if len(tables) != 2 {
		t.Error("GetAllTables not working", tables)
	}
}

func TestManyToMany(t *testing.T) {
	err := ManyToMany("users", "groups")
	if err != nil {
		t.Error(err)
	}
	found := false
	for _, t := range GetAllTables() {
		if t == "m2m_users_groups" {
			found = true
		}
	}
	if !found {
		t.Error("m2m_users_groups has not been created:", GetAllTables())
	}
}

func TestInsertUsersAndGroups(t *testing.T) {
	for i := 0; i < 10; i++ {
		iString := strconv.Itoa(i)
		email := "user-" + iString + "@example.com"
		admin := true
		_, err := Model[TestUser]().Insert(&TestUser{
			Uuid:      GenerateUUID(),
			Email:     &email,
			Password:  "dqdqd",
			IsAdmin:   &admin,
			CreatedAt: time.Now(),
		})
		if err != nil {
			t.Error(err)
		}
	}
	_, err := Model[Group]().BulkInsert(&Group{
		Name: "admin",
	}, &Group{Name: "normal"})
	if err != nil {
		t.Error(err)
	}
	_, err = Table("groups").BulkInsert(map[string]any{
		"name": "another",
	}, map[string]any{
		"name": "last",
	})
	if err != nil {
		t.Error(err)
	}
}

func TestAddRelatedS(t *testing.T) {
	_, err := Model[Group]().Where("name = ?", "admin").AddRelated("users", "id = ?", 1)
	if err != nil {
		t.Error(err)
	}
	_, err = Model[Group]().Where("name = ?", "admin").AddRelated("users", "id = ?", 2)
	if err != nil {
		t.Error(err)
	}
}

func TestAddRelatedM(t *testing.T) {
	_, err := Table("users").Where("id = ?", 3).AddRelated("groups", "name = ?", "admin")
	if err != nil {
		t.Error(err)
	}
	_, err = Table("users").Where("id = ?", 4).AddRelated("groups", "name = ?", "admin")
	if err != nil {
		t.Error(err)
	}
}

func TestDeleteRelatedS(t *testing.T) {
	_, err := Model[Group]().Where("name = ?", "admin").DeleteRelated("users", "id = ?", 1)
	if err != nil {
		t.Error(err)
	}
}

func TestDeleteRelatedM(t *testing.T) {
	_, err := Table("groups").Where("name = ?", "admin").DeleteRelated("users", "id = ?", 2)
	if err != nil {
		t.Error(err)
	}
}

func TestGetRelatedM(t *testing.T) {
	users := []map[string]any{}
	err := Table("groups").Where("name", "admin").GetRelated("users", &users)
	if err != nil {
		t.Error(err)
	}
	if len(users) != 2 {
		t.Error("len(users) != 2 , got: ", users)
	}
}

func TestGetRelatedS(t *testing.T) {
	users := []TestUser{}
	err := Model[Group]().Where("name = ?", "admin").GetRelated("users", &users)
	if err != nil {
		t.Error(err)
	}
	if len(users) != 2 {
		t.Error("len(users) != 2 , got: ", users)
	}
}

func TestConcatANDLen(t *testing.T) {
	groupes, err := Model[Group]().Where("name = concat(?,'min') AND len(name) = ?", "ad", 5).All()
	// translated to select * from groups WHERE name = 'ad' || 'min'  AND  length(name) = 5 (sqlite)
	// translated to select * from groups WHERE name = concat('ad','min')  AND  char_length(name) = 5 (postgres, mysql)
	if err != nil {
		t.Error(err)
	}
	if len(groupes) != 1 || groupes[0].Name != "admin" {
		t.Error("len(groupes) != 1 , got: ", groupes)
	}
}

func TestGetRelatedSWithLen(t *testing.T) {
	users := []TestUser{}
	err := Model[Group]().Where("name = ? AND len(name) = ?", "admin", 5).GetRelated("users", &users)
	if err != nil {
		t.Error(err)
	}
	if len(users) != 2 {
		t.Error("len(users) != 2 , got: ", users)
	}
}

func TestGetRelatedSWithConcatANDLen(t *testing.T) {
	users := []TestUser{}
	err := Model[Group]().Where("name = concat(?,'min') AND len(name) = ?", "ad", 5).GetRelated("users", &users)
	if err != nil {
		t.Error(err)
	}
	if len(users) != 2 {
		t.Error("len(users) != 2 , got: ", users)
	}
}

func TestGeneratedAs(t *testing.T) {
	u, err := Model[TestUser]().Limit(3).All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 3 {
		t.Error("len not 20")
	}
	if (u[0].Gen != u[0].Uuid+"working"+fmt.Sprintf("%d", len(u[0].Password))) || u[0].Gen2 != len(u[0].Password)*2 {
		t.Error("generated not working:", u[0].Gen)
	}
}

func TestJoinRelatedM(t *testing.T) {
	users := []map[string]any{}
	err := Table("groups").Where("name = ?", "admin").JoinRelated("users", &users)
	if err != nil {
		t.Error(err)
	}
	if len(users) != 2 {
		t.Error("len(users) != 2 , got: ", users)
	}
}

func TestInsertForeignKeyShouldError(t *testing.T) {
	for i := 0; i < 10; i++ {
		email := "user-0@example.com"
		admin := true
		_, err := Model[TestUser]().Insert(&TestUser{
			Uuid:      GenerateUUID(),
			Email:     &email,
			Password:  "dqdqd",
			IsAdmin:   &admin,
			CreatedAt: time.Now(),
		})
		if err == nil {
			t.Error("should error,did not")
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
			t.Error(err)
		}
	}
}

func TestGetAll(t *testing.T) {
	u, err := Model[TestUser]().All()
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

func TestQuery(t *testing.T) {
	u, err := Query(DB_TEST_NAME, "select * from users")
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("len not 20")
	}
}

func TestMemoryDatabases(t *testing.T) {
	dbs := GetMemoryDatabases()
	if len(dbs) != 1 {
		t.Error("len(dbs) != 1")
	}
	if dbs[0].Name != DB_TEST_NAME {
		t.Error("dbs[0].Name != DB_TEST_NAME:", dbs[0].Name)
	}
}

func TestMemoryDatabase(t *testing.T) {
	db, err := GetMemoryDatabase(DB_TEST_NAME)
	if err != nil {
		t.Error(err)
	}
	if db.Name != DB_TEST_NAME {
		t.Error("db.Name != DB_TEST_NAME:", db.Name)
	}
}

func TestGetOne(t *testing.T) {
	u, err := Model[TestUser]().Where("id = ?", 1).One()
	if err != nil {
		t.Error(err)
	}
	if !*u.IsAdmin || *u.Email == "" || u.CreatedAt.IsZero() || u.Uuid == "" {
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
	u, err := Model[TestUser]().Debug().Where("id = ?", 1).One()
	if err != nil {
		t.Error(err)
	}
	if !*u.IsAdmin || *u.Email == "" || u.CreatedAt.IsZero() || u.Uuid == "" {
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
	u, err := Model[TestUser]().Where("is_admin = ?", true).OrderBy("-id").All()
	if err != nil {
		t.Error(err)
	}
	if (len(u) > 1 && *(u[0]).Id < *(u[1]).Id) || !*(u[0]).IsAdmin || *(u[0]).Email == "" || u[0].CreatedAt.IsZero() || u[0].Uuid == "" {
		t.Error("wrong data:", u[0], u[0].CreatedAt.IsZero())
	}
}

func TestOrderByM(t *testing.T) {
	u, err := Table("users").Where("is_admin = ?", true).OrderBy("-id").All()
	if err != nil {
		t.Error(err)
	}
	if (len(u) > 1 && u[0]["id"].(int64) < u[1]["id"].(int64)) || u[0]["is_admin"] != int64(1) || u[0]["email"] == "" || u[0]["uuid"] == "" {
		t.Error("wrong data:", u[0])
	}
}

func TestPagination(t *testing.T) {
	u, err := Model[TestUser]().Where("is_admin = ?", true).Limit(5).Page(2).All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 5 || *(u[0]).Id != 6 {
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
	u, err := Model[TestUser]().Where("is_admin = ?", true).Context(context.Background()).All()
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

func TestQueryS(t *testing.T) {
	u, err := QueryS[TestUser]("", "select * from users")
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("missing data")
	}
}

func TestQueryM(t *testing.T) {
	u, err := Query("", "select * from users")
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 {
		t.Error("missing data")
	}
}

func TestSelect(t *testing.T) {
	u, err := Model[TestUser]().Select("email").All()
	if err != nil {
		t.Error(err)
	}
	if len(u) != 20 || !u[0].CreatedAt.IsZero() || *u[0].Email == "" || u[0].Password != "" {
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

func TestDatabase(t *testing.T) {
	u, err := Model[TestUser]().Database(DB_TEST_NAME).All()
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
	is_admin := true
	n, err := Model[TestUser]().Where("id = ?", 3).Set("email,is_admin", updatedEmail, &is_admin)
	if err != nil {
		t.Error(err)
	}
	if n <= 0 {
		t.Error("nothing updated, it should")
	}
	u, err := Model[TestUser]().Where("id = ?", 3).One()
	if err != nil {
		t.Error(err)
	}
	if *u.Email != updatedEmail || !*u.IsAdmin {
		t.Errorf("expect %s got %v, bool is %v", updatedEmail, *u.Email, *u.IsAdmin)
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
	u, err := Model[TestUser]().Where("id = ?", 7).One()
	if err != nil {
		t.Error(err)
	}
	if *u.Email != updatedEmail {
		t.Errorf("expect %s got %v", updatedEmail, u.Email)
	}
}

func TestDelete(t *testing.T) {
	n, err := Model[TestUser]().Where("id = ?", 12).Delete()
	if err != nil {
		t.Error(err)
	}
	if n < 0 {
		t.Error("nothing deleted, it should", n)
	}
	u, err := Model[TestUser]().Where("id = ?", 12).One()
	if err == nil {
		t.Error("not errored, it should : ", err, u)
	}
}

func TestDeleteM(t *testing.T) {
	n, err := Table("users").Where("id = ?", 13).Delete()
	if err != nil {
		t.Error(err)
	}
	if n < 0 {
		t.Error("nothing deleted, it should")
	}
	_, err = Table("users").Where("id = ?", 12).One()
	if err == nil {
		t.Error("not errored, it should")
	}
}

func TestDropM(t *testing.T) {
	_, err := Table("m2m_users_groups").Drop()
	if err != nil {
		t.Error(err)
	}
	for _, table := range GetAllTables() {
		if table == "m2m_users_groups groups" {
			t.Error("m2m_users_groups groups table not dropped", GetAllTables())
		}
	}
}

func TestDropS(t *testing.T) {
	_, err := Model[TestUser]().Drop()
	if err != nil {
		t.Error(err)
	}
	for _, table := range GetAllTables() {
		if table == "users" {
			t.Error("users table not dropped", GetAllTables())
		}
	}
}

func TestShutdown(t *testing.T) {
	err := Shutdown(DB_TEST_NAME)
	if err != nil {
		t.Error(err)
	}
}
