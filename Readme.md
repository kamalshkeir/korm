<div align="center">
	<img src="./korm.png" width="auto" style="margin:0 auto 0 auto;"/>
</div>
<br>
<div align="center">
	<img src="https://img.shields.io/github/go-mod/go-version/kamalshkeir/korm" width="auto" height="20px">
	<img src="https://img.shields.io/github/languages/code-size/kamalshkeir/korm" width="auto" height="20px">
	<img src="https://img.shields.io/badge/License-BSD%20v3-blue.svg" width="auto" height="20px">
	<img src="https://img.shields.io/github/v/tag/kamalshkeir/korm" width="auto" height="20px">
	<img src="https://img.shields.io/github/stars/kamalshkeir/korm?style=social" width="auto" height="20px">
	<img src="https://img.shields.io/github/forks/kamalshkeir/korm?style=social" width="auto" height="20px">
</div>
<br>




<div align="center">
	<a href="https://kamalshkeir.dev" target="_blank">
		<img src="https://img.shields.io/badge/my_portfolio-000?style=for-the-badge&logo=ko-fi&logoColor=white" width="auto" height="32px">
	</a>
	<a href="https://www.linkedin.com/in/kamal-shkeir/">
		<img src="https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white" width="auto" height="30px">
	</a>
	<a href="https://www.buymeacoffee.com/kamalshkeir" target="_blank"><img src="https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png" alt="Buy Me A Coffee" width="auto" height="32px" ></a>

	
</div>

---
### KORM is an elegant and blazingly fast ORM, see [Benchmarks](#benchmarks), it use go generics 1.18 and a network bus.

### Easily composable, you can combine it with a Server Bus using [WithBus](#example-with-bus-between-2-korm) when you want to scale or just synchronise your data between multiple database or [WithDashboard](#example-with-dashboard-you-dont-need-kormwithbus-with-it-because-withdashboard-already-call-it-and-return-the-server-bus-for-you) to have a complete setup of server bus and Admin Dashboard.

##### It can handle sql databases and Mongo using [Kormongo](https://github.com/kamalshkeir/kormongo), both have pretty much the same api, everything detailed in this readme
##### All drivers are written in Go, so you will never encounter gcc or c missing compiler

### It Has :
- New: [Hooks](#hooks) : OnInsert OnSet OnDelete and OnDrop
- Simple [API](#api)
- CRUD [Admin dashboard](#example-with-dashboard-you-dont-need-kormwithbus-with-it-because-withdashboard-already-call-it-and-return-the-server-bus-for-you) with ready offline installable PWA (using /static/sw.js and /static/manifest.json). All statics mentionned in `sw.js` will be cached and served by the service worker, you can inspect the Network to check it. 
- [Router/Mux](https://github.com/kamalshkeir/kmux) accessible from the serverBus after calling `korm.WithBus()` or `korm.WithDashboard()`
- [PPROF](#pprof) Go std library profiling tool
- [Kenv](#example-not-required-load-config-from-env-directly-to-struct-using-kenv) load env vars to struct
- [many to many](#manytomany-relationships-example) relationships 
- Support for foreign keys, indexes , checks,... [See all](#automigrate)
- [Interactive Shell](#interactive-shell), to CRUD in your databases `go run main.go shell` or `go run main.go mongoshell` for mongo
- Network Bus allowing you to send and recv data in realtime using pubsub websockets between your ORMs, so you can decide how you data will be distributed between different databases, see [Example](#example-with-bus-between-2-korm) .
- Compatible with std library database/sql, and the Mongo official driver, so if you want, know that you can always do your queries yourself using sql.DB or mongo.Client  `korm.GetConnection(dbName)` or `kormongo.GetConnection(dbName)`
- [AutoMigrate](#automigrate) directly from struct, for mongo it will only link the struct to the tableName, allowing usage of BuilderS. For all sql, whenever you add or remove a field from a migrated struct, you will get a prompt proposing to add the column for the table in the database or remove a column, you can also only generate the query without execute, and then you can use the shell to migrate the generated file, to disable the check for sql, you can use `korm.DisableCheck()`.
- [Load config](#load-config-from-env-directly-to-struct-using-kenv) from env directly to struct
- Concurrency Safe access.


#### Supported databases:
- Sqlite
- Postgres
- Mysql
- Maria
- Coakroach
- Mongo via [MONGO](https://github.com/kamalshkeir/kormongo)


---
# Installation

```sh
go get -u github.com/kamalshkeir/korm@v1.3.9 // latest version
```

# Drivers moved outside this package to not get them all in your go.mod file
```sh
go get github.com/kamalshkeir/sqlitedriver
go get github.com/kamalshkeir/pgdriver
go get github.com/kamalshkeir/mysqldriver
```

```sh
go get -u github.com/kamalshkeir/kormongo@latest // Mongo ORM
```

### Global Vars
```go
// Debug when true show extra useful logs for queries executed for migrations and queries statements
Debug = false
// FlushCacheEvery execute korm.FlushCache() every 10 min by default, you should not worry about it, but useful that you can change it
FlushCacheEvery = 10 * time.Minute
// Connection pool
MaxOpenConns = 20
MaxIdleConns = 7
MaxLifetime = 1 * time.Hour
MaxIdleTime = 1 * time.Hour
```

### Connect to a database
```go
// mongodb
err := kormongo.New("dbmongo", "localhost:27017")
// sqlite
sqlitedriver.Use() // load sqlite driver --> go get github.com/kamalshkeir/sqlitedriver
err := korm.New(korm.SQLITE, "db") // Connect
// postgres, coakroach
pgdriver.Use() // load postgres driver  --> go get github.com/kamalshkeir/pgdriver
err := korm.New(korm.POSTGRES,"dbName", "user:password@localhost:5432") // Connect
// mysql, maria
mysqldriver.Use() // load mysql driver  --> go get github.com/kamalshkeir/mysqldriver
err := korm.New(korm.MYSQL,"dbName","user:password@localhost:3306") // Connect

korm.Shutdown(databasesName ...string) error
kormongo.ShutdownDatabases(databasesName ...string) error
```




### AutoMigrate 

[Available Tags](#available-tags-by-struct-field-type) (SQL)

SQL:
```go
korm.AutoMigrate[T comparable](tableName string, dbName ...string) error 

err := korm.AutoMigrate[User]("users")
err := korm.AutoMigrate[Bookmark ]("bookmarks")

type User struct {
	Id        int       `korm:"pk"` // AUTO Increment ID primary key
	Uuid      string    `korm:"size:40"` // VARCHAR(50)
	Email     string    `korm:"size:50;iunique"` // insensitive unique
	Password  string    `korm:"size:150"` // VARCHAR(150)
	IsAdmin   bool      `korm:"default:false"` // DEFAULT 0
	Image     string    `korm:"size:100;default:''"`
	CreatedAt time.Time `korm:"now"` // auto now
    Ignored   string    `korm:"-"`
}

type Bookmark struct {
	Id      uint   `korm:"pk"`
	UserId  int    `korm:"fk:users.id:cascade:setnull"` // options cascade,donothing/noaction, setnull/null, setdefault/default
	IsDone	bool   
	ToCheck string `korm:"size:50; notnull; check: len(to_check) > 2 AND len(to_check) < 10; check: is_done=true"`  // column type will be VARCHAR(50)
	Content string `korm:"text"` // column type will be TEXT not VARCHAR
	UpdatedAt time.Time `korm:"update"` // will update when model updated, handled by triggers for sqlite, coakroach and postgres, and on migration for mysql
	CreatedAt time.Time `korm:"now"` // now is default to current timestamp and of type TEXT for sqlite
}

all, _ := korm.Model[User]()
                   .Where("id = ?",id) // notice here not like mongo, mongo will be like Where("_id",id) without '= ?'
                   .Select("item1","item2")
                   .OrderBy("created")
				   .Limit(8)
				   .Page(2)
                   .All()
```

MONGO: (No TAGS), only primitive.ObjectID `bson:"_id"` is mandatory
```go
type FirstTable struct {
	Id      primitive.ObjectID `bson:"_id"`
	Num     uint
	Item1   string
	Item2   string
	Bool1   bool
	Created time.Time
}

err = korm.AutoMigrate[FirstTable]("first_table")
klog.CheckError(err)

id,_ := primitive.ObjectIDFromHex("636d4c7bcfde1f5b625f12a4")
all, _ := korm.Model[FirstTable]()
                   .Where("_id",id) // notice here for mongo it's not like sql Where("_id = ?",id) 
                   .Select("item1","item2")
                   .OrderBy("created")
				   .Limit(8)
				   .Page(2)
                   .All()
```

### API
#### General
```go
func New(dbType, dbName string, dbDSN ...string) error
func NewFromConnection(dbType, dbName string, conn *sql.DB) error
func NewFromConnection(dbName string,dbConn *mongo.Database) error (kormongo)
func Exec(dbName, query string, args ...any) error
func Transaction(dbName ...string) (*sql.Tx, error)
func WithBus(bus *ksbus.Server) *ksbus.Server // Usage: WithBus(ksbus.NewServer()) or share an existing one
func WithDashboard(staticAndTemplatesEmbeded ...embed.FS) *ksbus.Server
func BeforeServersData(fn func(data any, conn *ws.Conn))
func BeforeDataWS(fn func(data map[string]any, conn *ws.Conn, originalRequest *http.Request) bool)
func GetConnection(dbName ...string) *sql.DB
func GetAllTables(dbName ...string) []string
func GetAllColumnsTypes(table string, dbName ...string) map[string]string
func GetMemoryTable(tbName string, dbName ...string) (TableEntity, error)
func GetMemoryTables(dbName ...string) ([]TableEntity, error)
func GetMemoryDatabases() []DatabaseEntity
func GetMemoryDatabase(dbName string) (*DatabaseEntity, error)
func Shutdown(databasesName ...string) error
func FlushCache()
func DisableCheck() // Korm Only, disable struct check on change to add or remove column
func DisableCache()
func ManyToMany(table1, table2 string, dbName ...string) error // add table relation m2m 
```
#### Builder `Struct`:
```go
korm.Exec(dbName, query string, args ...any) error
korm.Transaction(dbName ...string) (*sql.Tx, error)
// Model is a starter for Buider
func Model[T comparable](tableName ...string) *BuilderS[T]
// Database allow to choose database to execute query on
func (b *BuilderS[T]) Database(dbName string) *BuilderS[T]
// Insert insert a row into a table and return inserted PK
func (b *BuilderS[T]) Insert(model *T) (int, error)
// InsertR add row to a table using input struct, and return the inserted row
func (b *BuilderS[T]) InsertR(model *T) (T, error)
// BulkInsert insert many row at the same time in one query
func (b *BuilderS[T]) BulkInsert(models ...*T) ([]int, error)
// AddRelated used for many to many, and after korm.ManyToMany, to add a class to a student or a student to a class, class or student should exist in the database before adding them
func (b *BuilderS[T]) AddRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error)
// DeleteRelated delete a relations many to many
func (b *BuilderS[T]) DeleteRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error)
// GetRelated used for many to many to get related classes to a student or related students to a class
func (b *BuilderS[T]) GetRelated(relatedTable string, dest any) error
// JoinRelated same as get, but it join data
func (b *BuilderS[T]) JoinRelated(relatedTable string, dest any) error
// Set used to update, Set("email,is_admin","example@mail.com",true) or Set("email = ? AND is_admin = ?","example@mail.com",true)
func (b *BuilderS[T]) Set(query string, args ...any) (int, error)
// Delete data from database, can be multiple, depending on the where, return affected rows(Not every database or database driver may support affected rows)
func (b *BuilderS[T]) Delete() (int, error)
// Drop drop table from db
func (b *BuilderS[T]) Drop() (int, error)
// Select usage: Select("email","password")
func (b *BuilderS[T]) Select(columns ...string) *BuilderS[T]
// Where can be like : Where("id > ?",1) or Where("id",1) = Where("id = ?",1)
func (b *BuilderS[T]) Where(query string, args ...any) *BuilderS[T]
// Query can be used like: Query("select * from table") or Query("select * from table where col like '?'","%something%")
func (b *BuilderS[T]) Query(query string, args ...any) *BuilderS[T]
// Limit set limit
func (b *BuilderS[T]) Limit(limit int) *BuilderS[T]
// Context allow to query or execute using ctx
func (b *BuilderS[T]) Context(ctx context.Context) *BuilderS[T]
// Page return paginated elements using Limit for specific page
func (b *BuilderS[T]) Page(pageNumber int) *BuilderS[T]
// OrderBy can be used like: OrderBy("-id","-email") OrderBy("id","-email") OrderBy("+id","email")
func (b *BuilderS[T]) OrderBy(fields ...string) *BuilderS[T]
// Debug print prepared statement and values for this operation
func (b *BuilderS[T]) Debug() *BuilderS[T]
// All get all data
func (b *BuilderS[T]) All() ([]T, error)
// One get single row
func (b *BuilderS[T]) One() (T, error)

Examples:
korm.Model[models.User]().Select("email","uuid").OrderBy("-id").Limit(PAGINATION_PER).Page(1).All()

// INSERT
uuid,_ := korm.GenerateUUID()
hashedPass,_ := argon.Hash(password)
korm.Model[models.User]().Insert(&models.User{
	Uuid: uuid,
	Email: "test@example.com",
	Password: hashedPass,
	IsAdmin: false,
	Image: "",
	CreatedAt: time.Now(),
})

//if using more than one db
korm.Database[models.User]("dbNameHere").Where("id = ? AND email = ?",1,"test@example.com").All() 

// where
korm.Model[models.User]().Where("id = ? AND email = ?",1,"test@example.com").One() 

// delete
korm.Model[models.User]().Where("id = ? AND email = ?",1,"test@example.com").Delete()

// drop table
korm.Model[models.User]().Drop()

// update
korm.Model[models.User]().Where("id = ?",1).Set("email = ?","new@example.com")
```
#### Builder `map[string]any`:
```go
// BuilderM is query builder map string any
type BuilderM struct
// Table is a starter for BuiderM
func Table(tableName string) *BuilderM
// Database allow to choose database to execute query on
func (b *BuilderM) Database(dbName string) *BuilderM
// Select select table columns to return
func (b *BuilderM) Select(columns ...string) *BuilderM
// Where can be like: Where("id > ?",1) or Where("id",1) = Where("id = ?",1)
func (b *BuilderM) Where(query string, args ...any) *BuilderM
// Query can be used like: Query("select * from table") or Query("select * from table where col like '?'","%something%")
func (b *BuilderM) Query(query string, args ...any) *BuilderM
// Limit set limit
func (b *BuilderM) Limit(limit int) *BuilderM
// Page return paginated elements using Limit for specific page
func (b *BuilderM) Page(pageNumber int) *BuilderM
// OrderBy can be used like: OrderBy("-id","-email") OrderBy("id","-email") OrderBy("+id","email")
func (b *BuilderM) OrderBy(fields ...string) *BuilderM
// Context allow to query or execute using ctx
func (b *BuilderM) Context(ctx context.Context) *BuilderM
// Debug print prepared statement and values for this operation
func (b *BuilderM) Debug() *BuilderM
// All get all data
func (b *BuilderM) All() ([]map[string]any, error)
// One get single row
func (b *BuilderM) One() (map[string]any, error)
// Insert add row to a table using input map, and return PK of the inserted row
func (b *BuilderM) Insert(rowData map[string]any) (int, error)
// InsertR add row to a table using input map, and return the inserted row
func (b *BuilderM) InsertR(rowData map[string]any) (map[string]any, error)
// BulkInsert insert many row at the same time in one query
func (b *BuilderM) BulkInsert(rowsData ...map[string]any) ([]int, error)
// Set used to update, Set("email,is_admin","example@mail.com",true) or Set("email = ? AND is_admin = ?","example@mail.com",true)
func (b *BuilderM) Set(query string, args ...any) (int, error)
// Delete data from database, can be multiple, depending on the where, return affected rows(Not every database or database driver may support affected rows)
func (b *BuilderM) Delete() (int, error)
// Drop drop table from db
func (b *BuilderM) Drop() (int, error)
// AddRelated used for many to many, and after korm.ManyToMany, to add a class to a student or a student to a class, class or student should exist in the database before adding them
func (b *BuilderM) AddRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error)
// GetRelated used for many to many to get related classes to a student or related students to a class
func (b *BuilderM) GetRelated(relatedTable string, dest *[]map[string]any) error
// JoinRelated same as get, but it join data
func (b *BuilderM) JoinRelated(relatedTable string, dest *[]map[string]any) error
// DeleteRelated delete a relations many to many
func (b *BuilderM) DeleteRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error)


Examples:

sliceMapStringAny,err := korm.Table("users")
							.Select("email","uuid")
							.OrderBy("-id")
							.Limit(PAGINATION_PER)
							.Page(1)
							.All()

// INSERT
uuid,_ := korm.GenerateUUID()
hashedPass,_ := argon.Hash("password") // github.com/kamalshkeir/argon

korm.Table("users").Insert(map[string]any{
	"uuid":uuid,
	"email":"test@example.com",
	 ...
})

//if using more than one db
korm.Database("dbNameHere").Table("tableName").Where("id = ? AND email = ?",1,"test@example.com").All() 

// where
Where("id = ? AND email = ?",1,"test@example.com") // this work
Where("id,email",1,"test@example.com") // and this work

korm.Table("tableName").Where("id = ? AND email = ?",1,"test@example.com").One() // SQL
kormongo.Table("tableName").Where("id, email",1,"test@example.com").One() // Mongo

// delete
korm.Table("tableName").Where("id = ? AND email = ?",1,"test@example.com").Delete() // SQL
kormongo.Table("tableName").Where("id,email", 1, "test@example.com").Delete() // Mongo

// drop table
korm.Table("tableName").Drop()

// update
korm.Table("tableName").Where("id = ?",1).Set("email = ?","new@example.com") // SQL 
korm.Table("tableName").Where("id",1).Set("email","new@example.com") 

korm.Table("tableName").Where("id",1).Set("email","new@example.com") // Mongo
```

### Dashboard defaults you can set
```go
korm.Pprof              = false
korm.PaginationPer      = 10
korm.EmbededDashboard   = false
korm.MediaDir           = "media"
korm.AssetsDir          = "assets"
korm.StaticDir          = path.Join(AssetsDir, "/", "static")
korm.TemplatesDir       = path.Join(AssetsDir, "/", "templates")
korm.RepoUser           = "kamalshkeir"
korm.RepoName           = "korm-dashboard"
korm.AdminPathNameGroup = "/admin"
// so you can create a custom dashboard, upload it to your repos and change like like above korm.RepoUser and korm.RepoName
```

### Example With Dashboard (you don't need korm.WithBus with it, because WithDashboard already call it and return the server bus for you)

```go
package main

import (
	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmux"
	"github.com/kamalshkeir/korm"
	"github.com/kamalshkeir/sqlitedriver"
)

func main() {
	sqlitedriver.Use()
	err := korm.New(korm.SQLITE, "db")
	klog.CheckError(err)



	serverBus := korm.WithDashboard() 
	// you can overwrite Admin and Auth middleware used for dashboard (dash_middlewares.go) 
	//korm.Auth = func(handler kmux.Handler) kmux.Handler {}
	//korm.Admin = func(handler kmux.Handler) kmux.Handler {}

	// and also all handlers (dash_views.go)
	//korm.LoginView = func(c *kmux.Context) {
	//	c.Html("admin/new_admin_login.html", nil)
	//}

	// add extra static directory if you want
	//serverBus.App.LocalStatics("assets/mystatic","myassets") // will be available at /myassets/*
	//serverBus.App.LocalTemplates("assets/templates") // will make them available to use with c.Html

	// serve HTML 
	// serverBus.App.Get("/",func(c *kmux.Context) {
	// 	c.Html("index.html", map[string]any{
	// 		"data": data,
	// 	})
	// })
	serverBus.Run("localhost:9313")
	// OR run https if you have certificates
	serverBus.RunTLS(addr string, cert string, certKey string)

	// OR generate certificates let's encrypt for a domain name, check https://github.com/kamalshkeir/ksbus for more infos
	serverBus.RunAutoTLS(domainName string, subDomains ...string)
}
```
Then create admin user to connect to the dashboard
```sh
go run main.go shell

createsuperuser
```

Then you can visit `/admin`


### Admin middlewares

```go
// dash_middlewares.go
package korm

import (
	"context"
	"net/http"

	"github.com/kamalshkeir/aes"
	"github.com/kamalshkeir/kmux"
)

var Auth = func(handler kmux.Handler) kmux.Handler {
	const key kmux.ContextKey = "user"
	return func(c *kmux.Context) {
		session, err := c.GetCookie("session")
		if err != nil || session == "" {
			// NOT AUTHENTICATED
			c.DeleteCookie("session")
			handler(c)
			return
		}
		session, err = aes.Decrypt(session)
		if err != nil {
			handler(c)
			return
		}
		// Check session
		user, err := Model[User]().Where("uuid = ?", session).One()
		if err != nil {
			// session fail
			handler(c)
			return
		}

		// AUTHENTICATED AND FOUND IN DB
		ctx := context.WithValue(c.Request.Context(), key, user)
		*c = kmux.Context{
			Params:         c.ParamsMap(),
			Request:        c.Request.WithContext(ctx),
			ResponseWriter: c.ResponseWriter,
		}
		handler(c)
	}
}

var Admin = func(handler kmux.Handler) kmux.Handler {
	const key kmux.ContextKey = "user"
	return func(c *kmux.Context) {
		session, err := c.GetCookie("session")
		if err != nil || session == "" {
			// NOT AUTHENTICATED
			c.DeleteCookie("session")
			c.Status(http.StatusTemporaryRedirect).Redirect("/admin/login")
			return
		}
		session, err = aes.Decrypt(session)
		if err != nil {
			c.Status(http.StatusTemporaryRedirect).Redirect("/admin/login")
			return
		}
		user, err := Model[User]().Where("uuid = ?", session).One()

		if err != nil {
			// AUTHENTICATED BUT NOT FOUND IN DB
			c.Status(http.StatusTemporaryRedirect).Redirect("/admin/login")
			return
		}

		// Not admin
		if !user.IsAdmin {
			c.Status(403).Text("Middleware : Not allowed to access this page")
			return
		}

		ctx := context.WithValue(c.Request.Context(), key, user)
		*c = kmux.Context{
			Params:         c.ParamsMap(),
			Request:        c.Request.WithContext(ctx),
			ResponseWriter: c.ResponseWriter,
		}

		handler(c)
	}
}

```

### Example With Bus between 2 KORM
KORM 1:

```go
package main

import (
	"net/http"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmux"
	"github.com/kamalshkeir/kmux/ws"
	"github.com/kamalshkeir/korm"
	"github.com/kamalshkeir/ksbus"
)

func main() {
	err := korm.New(korm.SQLITE,"db1")
	if klog.CheckError(err) {return}

	
	serverBus := korm.WithBus(ksbus.NewServer())
	// handler authentication	
	korm.BeforeDataWS(func(data map[string]any, conn *ws.Conn, originalRequest *http.Request) bool {
        klog.Printf("handle authentication here\n")
		return true
	})
	// handler data from other KORM
	korm.BeforeServersData(func(data any, conn *ws.Conn) {
		klog.Printf("grrecv orm2: %v\n",data) // 'gr' for green
	})

	// built in router to the bus, check it at https://github.com/kamalshkeir/ksbus
	serverBus.App.GET("/",func(c *kmux.Context) {
		serverBus.SendToServer("localhost:9314",map[string]any{
			"msg":"hello from server 1",
		})
		c.Text("ok")
	})

	
	serverBus.Run("localhost:9313")
	// OR run https if you have certificates
	serverBus.RunTLS(addr string, cert string, certKey string)
	// OR generate certificates let's encrypt for a domain name, check https://github.com/kamalshkeir/ksbus for more details
	serverBus.RunAutoTLS(domainName string, subDomains ...string)
}
```
KORM 2:
```go
package main

import (
	"net/http"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmux"
	"github.com/kamalshkeir/kmux/ws"
	"github.com/kamalshkeir/korm"
)

func main() {
	err := korm.New(korm.SQLITE,"db2")
	if klog.CheckError(err) {return}

	
	serverBus := korm.WithBus(ksbus.NewServer())

	korm.BeforeServersData(func(data any, conn *ws.Conn) {
        klog.Printf("grrecv orm2: %v\n",data)
	})

	// built in router to the bus, check it at https://github.com/kamalshkeir/ksbus
	serverBus.App.GET("/",func(c *kmux.Context) {
		serverBus.SendToServer("localhost:9314",map[string]any{
			"msg":"hello from server 2",
		})
		c.Status(200).Text("ok")
	})


    // Run Server Bus
	serverBus.Run("localhost:9314")

	// OR run https if you have certificates
	serverBus.RunTLS(addr string, cert string, certKey string)

	// OR generate certificates let's encrypt for a domain name, check https://github.com/kamalshkeir/ksbus for more infos
	serverBus.RunAutoTLS(domainName string, subDomains ...string)
}
```

## Router/Mux github.com/kamalshkeir/kmux
```go

func main() {
	sqlitedriver.Use()
	err := korm.New(korm.SQLITE, "db")
	if err != nil {
		log.Fatal(err)
	}

	serverBus := korm.WithDashboard()

	mux := serverBus.App
	// add global middlewares
	mux.Use((midws ...func(http.Handler) http.Handler))
	mux.Use(kmux.Gzip(),kmux.Recover())
	...
}

```

### Pprof
```go
korm.Pprof=true (before WithDashboard)
will enable:
	- /debug/pprof
	- /debug/pprof/profile
	- /debug/pprof/heap
	- /debug/pprof/trace
```

# Hooks
```go
korm.OnInsert(func(database, table string, data map[string]any) error {
	fmt.Println("inserting into", database, table, data)
	// if error returned, it will not insert
	return nil
})

korm.OnSet(func(database, table string, data map[string]any) error {
	fmt.Println("set into", database, table, data)
	return nil
})

korm.OnDelete(func(database, table, query string, args ...any) error {})

korm.OnDrop(func(database, table string) error {})
```


# ManyToMany Relationships Example

```go
type Class struct {
	Id          uint   `korm:"pk"`
	Name        string `korm:"size:100"`
	IsAvailable bool
	CreatedAt   time.Time `korm:"now"`
}

type Student struct {
	Id        uint      `korm:"pk"`
	Name      string    `korm:"size:100"`
	CreatedAt time.Time `korm:"now"`
}

// migrate
func migrate() {
	err := korm.AutoMigrate[Class]("classes")
	if klog.CheckError(err) {
		return
	}
	err = korm.AutoMigrate[Student]("students")
	if klog.CheckError(err) {
		return
	}
	err = korm.ManyToMany("classes", "students")
	if klog.CheckError(err) {
		return
	}
}

// korm.ManyToMany create relation table named m2m_classes_students

// then you can use it like so to get related data

// get related to map to struct
std := []Student{}
err = korm.Model[Class]().Where("name = ?", "Math").Select("name").OrderBy("-name").Limit(1).GetRelated("students", &std)

// get related to map
std := []map[string]any{}
err = korm.Table("classes").Where("name = ?", "Math").Select("name").OrderBy("-name").Limit(1).GetRelated("students", &std)

// join related to map
std := []map[string]any{}
err = korm.Table("classes").Where("name = ?", "Math").JoinRelated("students", &std)

// join related to strcu
cu := []JoinClassUser{}
err = korm.Model[Class]().Where("name = ?", "Math").JoinRelated("students", &cu)

// to add relation
_, err = korm.Model[Class]().AddRelated("students", "name = ?", "hisName")
_, err = korm.Model[Student]().AddRelated("classes", "name = ?", "French")
_, err = korm.Table("students").AddRelated("classes", "name = ?", "French")

// delete relation
_, err = korm.Model[Class]().Where("name = ?", "Math").DeleteRelated("students", "name = ?", "hisName")
_, err = korm.Table("classes").Where("name = ?", "Math").DeleteRelated("students", "name = ?", "hisName")

```


# Example, not required, Load config from env directly to struct using Kenv
```go
import "github.com/kamalshkeir/kenv"

type EmbedS struct {
	Static    bool `kenv:"EMBED_STATIC|false"`
	Templates bool `kenv:"EMBED_TEMPLATES|false"`
}

type GlobalConfig struct {
	Host       string `kenv:"HOST|localhost"` // DEFAULT to 'localhost': if HOST not found in env
	Port       string `kenv:"PORT|9313"`
	Embed 	   EmbedS
	Db struct {
		Name     string `kenv:"DB_NAME|db"` // NOT REQUIRED: if DB_NAME not found, defaulted to 'db'
		Type     string `kenv:"DB_TYPE"` // REEQUIRED: this env var is required, you will have error if empty
		DSN      string `kenv:"DB_DSN|"` // NOT REQUIRED: if DB_DSN not found it's not required, it's ok to stay empty
	}
	Smtp struct {
		Email string `kenv:"SMTP_EMAIL|"`
		Pass  string `kenv:"SMTP_PASS|"`
		Host  string `kenv:"SMTP_HOST|"`
		Port  string `kenv:"SMTP_PORT|"`
	}
	Profiler   bool   `kenv:"PROFILER|false"`
	Docs       bool   `kenv:"DOCS|false"`
	Logs       bool   `kenv:"LOGS|false"`
	Monitoring bool   `kenv:"MONITORING|false"`
}


kenv.Load(".env") // load env file

// Fill struct from env loaded before:
Config := &GlobalConfig{}
err := kenv.Fill(Config) // fill struct with env vars loaded before
```



# Benchmarks
goos: windows
goarch: amd64
pkg: github.com/kamalshkeir/korm/benchmarks
cpu: Intel(R) Core(TM) i5-7300HQ CPU @ 2.50GHz
```go
////////////////////////////////////////////  query 5000 rows  //////////////////////////////////////////////
BenchmarkGetAllS_GORM-4               33          41601094 ns/op         8796852 B/op     234780 allocs/op
BenchmarkGetAllS-4               2771838               390.3 ns/op           224 B/op          1 allocs/op
BenchmarkGetAllM_GORM-4               25          44866500 ns/op         9433536 B/op     334631 allocs/op
BenchmarkGetAllM-4               4113112               268.6 ns/op           224 B/op          1 allocs/op
BenchmarkGetRowS_GORM-4            12170             97829 ns/op            5962 B/op        142 allocs/op
BenchmarkGetRowS-4               1448455               828.9 ns/op           336 B/op          7 allocs/op
BenchmarkGetRowM_GORM-4            11899            101547 ns/op            7096 B/op        200 allocs/op
BenchmarkGetRowM-4               1731766               693.2 ns/op           336 B/op          7 allocs/op
BenchmarkGetAllTables-4         47112411                25.61 ns/op            0 B/op          0 allocs/op
BenchmarkGetAllColumns-4        30015081                41.07 ns/op            0 B/op          0 allocs/op
////////////////////////////////////////////  query 1000 rows  //////////////////////////////////////////////
BenchmarkGetAllS_GORM-4              158           7131799 ns/op         1684076 B/op      46736 allocs/op
BenchmarkGetAllS-4               2665074               416.9 ns/op           224 B/op          1 allocs/op
BenchmarkGetAllM_GORM-4              130           8388724 ns/op         1887113 B/op      66626 allocs/op
BenchmarkGetAllM-4               3835689               294.8 ns/op           224 B/op          1 allocs/op
BenchmarkGetRowS_GORM-4            12292             95914 ns/op            5967 B/op        142 allocs/op
BenchmarkGetRowS-4               1324114               886.1 ns/op           336 B/op          7 allocs/op
BenchmarkGetRowM_GORM-4            10000            102954 ns/op            7096 B/op        200 allocs/op
BenchmarkGetRowM-4               1614579               754.4 ns/op           336 B/op          7 allocs/op
BenchmarkGetAllTables-4         42066442                25.67 ns/op            0 B/op          0 allocs/op
BenchmarkGetAllColumns-4        27996565                41.50 ns/op            0 B/op          0 allocs/op
////////////////////////////////////////////  query 100 rows  //////////////////////////////////////////////
BenchmarkGetAllS_GORM-4             1585            726960 ns/op          164736 B/op       4575 allocs/op
BenchmarkGetAllS-4               3050307               389.4 ns/op           224 B/op          1 allocs/op
BenchmarkGetAllM_GORM-4             1252            884975 ns/op          191158 B/op       6629 allocs/op
BenchmarkGetAllM-4               4131709               310.1 ns/op           224 B/op          1 allocs/op
BenchmarkGetRowS_GORM-4            11154             98986 ns/op            5966 B/op        142 allocs/op
BenchmarkGetRowS-4               1379994               873.6 ns/op           336 B/op          7 allocs/op
BenchmarkGetRowM_GORM-4            10000            106291 ns/op            7096 B/op        200 allocs/op
BenchmarkGetRowM-4               1652276               728.3 ns/op           336 B/op          7 allocs/op
BenchmarkGetAllTables-4         47458011                26.52 ns/op            0 B/op          0 allocs/op
BenchmarkGetAllColumns-4        27860600                42.02 ns/op            0 B/op          0 allocs/op
////////////////////////////////////////////    MONGO       //////////////////////////////////////////////
BenchmarkGetAllS-4               3121384               385.6 ns/op           224 B/op          1 allocs/op
BenchmarkGetAllM-4               4570059               264.2 ns/op           224 B/op          1 allocs/op
BenchmarkGetRowS-4               1404399               866.6 ns/op           336 B/op          7 allocs/op
BenchmarkGetRowM-4               1691026               722.6 ns/op           336 B/op          7 allocs/op
BenchmarkGetAllTables-4         47424489                25.34 ns/op            0 B/op          0 allocs/op
BenchmarkGetAllColumns-4        27039632                42.22 ns/op            0 B/op          0 allocs/op
//////////////////////////////////////////////////////////////////////////////////////////////////////////
```



---
### Available Tags by struct field type:

#String Field:
<table>
<tr>
<th>Without parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
<th>With parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
</tr>
<tr>
<td>
 
```
*  	text (create column as TEXT not VARCHAR)
*  	notnull
*  	unique
*   iunique // insensitive unique
*  	index, +index, index+ (INDEX ascending)
*  	index-, -index (INDEX descending)
*  	default (DEFAULT '')
```
</td>
<td>

```
* 	default:'any' (DEFAULT 'any')
*	mindex:...
* 	uindex:username,Iemail // CREATE UNIQUE INDEX ON users (username,LOWER(email)) 
	// 	email is lower because of 'I' meaning Insensitive for email
* 	fk:...
* 	size:50  (VARCHAR(50))
* 	check:...
```

</td>
</tr>
</table>


---



# Int, Uint, Int64, Uint64 Fields:
<table>
<tr>
<th>Without parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
</tr>
<tr>
<td>
 
```
*   -  			 (To Ignore a field)
*   autoinc, pk  (PRIMARY KEY)
*   notnull      (NOT NULL)
*  	index, +index, index+ (CREATE INDEX ON COLUMN)
*  	index-, -index(CREATE INDEX DESC ON COLUMN)     
*   unique 		 (CREATE UNIQUE INDEX ON COLUMN) 
*   default		 (DEFAULT 0)
```
</td>
</tr>

<tr><th>With parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th></tr>
<tr>
<td>

```
Available 'on_delete' and 'on_update' options: cascade,(donothing,noaction),(setnull,null),(setdefault,default)

*   fk:{table}.{column}:{on_delete}:{on_update} 
*   check: len(to_check) > 10 ; check: is_used=true (You can chain checks or keep it in the same CHECK separated by AND)
*   mindex: first_name, last_name (CREATE MULTI INDEX ON COLUMN + first_name + last_name)
*   uindex: first_name, last_name (CREATE MULTI UNIQUE INDEX ON COLUMN + first_name + last_name) 
*   default:5 (DEFAULT 5)
```

</td>
</tr>
</table>

---


# Bool : bool is INTEGER NOT NULL checked between 0 and 1 (in order to be consistent accross sql dialects)
<table>
<tr>
<th>Without parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
<th>With parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
</tr>
<tr>
<td>
 
```
*  	index, +index, index+ (CREATE INDEX ON COLUMN)
*  	index-, -index(CREATE INDEX DESC ON COLUMN)  
*   default (DEFAULT 0)
```
</td>
<td>

```
*   default:1 (DEFAULT 1)
*   mindex:...
*   fk:...
```

</td>
</tr>
</table>

---

# time.Time :
<table>
<tr>
<th>Without parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
<th>With parameter</th>
</tr>
<tr>
<td>
 
```
*  	index, +index, index+ (CREATE INDEX ON COLUMN)
*  	index-, -index(CREATE INDEX DESC ON COLUMN)  
*   now (NOT NULL and defaulted to current timestamp)
*   update (NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)
```
</td>
<td>

```
*   fk:...
*   check:...
```

</td>
</tr>
</table>

---

# Float64 :
<table>
<tr>
<th>Without parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
<th>With parameter&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
</tr>
<tr>
<td>
 
```
*   notnull
*  	index, +index, index+ (CREATE INDEX ON COLUMN)
*  	index-, -index(CREATE INDEX DESC ON COLUMN)  
*   unique
*   default
```
</td>
<td>

```
*   default:...
*   fk:...
*   mindex:...
*   uindex:...
*   check:...
```

</td>
</tr>
</table>

---

### Interactive shell
```shell
AVAILABLE COMMANDS:
[databases, use, tables, columns, migrate, createsuperuser, createuser, getall, get, drop, delete, clear/cls, q/quit/exit, help/commands]
  'databases':
	  list all connected databases

  'use':
	  use a specific database

  'tables':
	  list all tables in database

  'columns':
	  list all columns of a table

  'migrate':
	  migrate or execute sql file

  'createsuperuser': #only with korm.WithDashboard()
	  create a admin user
  
  'createuser':  #only with korm.WithDashboard()
	  create a regular user

  'getall':
	  get all rows given a table name

  'get':
	  get single row wher field equal_to

  'delete':
	  delete rows where field equal_to

  'drop':
	  drop a table given table name

  'clear/cls':
	  clear console
```


# 🔗 Links
[![portfolio](https://img.shields.io/badge/my_portfolio-000?style=for-the-badge&logo=ko-fi&logoColor=white)](https://kamalshkeir.dev/) [![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/kamal-shkeir/)


---

# Licence
Licence [BSD-3](./LICENSE)
