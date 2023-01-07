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
## KORM is an Elegant and Blazingly Fast ORM and migration tool, see [Benchmarks](#benchmarks), it use go generics 1.18 and a network bus.

### It is easily composable, you can combine it with a Server Bus (using WithBus) when you want to scale or just synchronise your data between multiple database or WithDashboard to have an admin panel

##### It can handle sql databases and Mongo using [Kormongo](https://github.com/kamalshkeir/kormongo), both have pretty much the same api, everything detailed in this readme
##### All drivers are written in Go, so you will never encounter gcc or c missing compiler


##### You have full control on the data came in and go out, you can check the example below [NetworkBus](#example-with-bus-between-2-korm)

### It Have :
- New: [Hooks](#hooks) : OnInsert OnSet OnDelete and OnDrop
- Simple [API](#api)
- [Admin dashboard](#example-with-dashboard) CRUD
- [many to many](#manytomany-relationships-example) relationships 
- Support for foreign keys, indexes , checks,... [See all](#automigrate)
- [Interactive Shell](#interactive-shell), to CRUD in your databases `go run main.go shell` or `go run main.go mongoshell` for mongo
- Network Bus allowing you to send and recv data in realtime using pubsub websockets between your ORMs, so you can decide how you data will be distributed between different databases, see [Example](#example-with-bus-between-2-korm) .
- It use std library database/sql, and the Mongo official driver, so if you want, know that you can always do your queries yourself using sql.DB or mongo.Client  `korm.GetConnection(dbName)` or `kormongo.GetConnection(dbName)`
- [AutoMigrate](#automigrate) directly from struct, for mongo it will only link the struct to the tableName, allowing usage of BuilderS. For all sql, whenever you add or remove a field from a migrated struct, you will get a prompt proposing to add the column for the table in the database or remove a column, you can also only generate the query without execute, and then you can use the shell to migrate the generated file.
- Powerful Query Builder for SQL and Mongo [Builder](#builder-mapstringany).
- Concurrency Safe access.


#### Supported databases:
- Postgres
- Mysql
- Mongo via [MONGO](https://github.com/kamalshkeir/kormongo)
- Sqlite
- Maria
- Coakroach


---
# Installation

```sh
go get -u github.com/kamalshkeir/korm@v1.3.7 // latest version
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
                   .All()
```

### API
#### General
```go
func New(dbType, dbName string, dbDSN ...string) error
func NewFromConnection(dbType, dbName string, conn *sql.DB) error
func NewFromConnection(dbName string,dbConn *mongo.Database) error (kormongo)
func WithBus(bus *ksbus.Server) *ksbus.Server // Usage: WithBus(ksbus.NewServer()) or share an existing one
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
func Exec(dbName, query string, args ...any) error
```
#### Builder `Struct`:
```go
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
hashedPass,_ := hash.GenerateHash("password")
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
hashedPass,_ := hash.GenerateHash("password")

korm.Model[models.User]().Insert(map[string]any{
	"uuid":uuid,
	"email:"test@example.com",
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
korm.Table("tableName").Where("id",1).Set("email","new@example.com") // orSQL 

korm.Table("tableName").Where("id",1).Set("email","new@example.com") // Mongo
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

	sbus := korm.WithDashboard() 
	// add extra static directory if you want
	//sbus.App.LocalStatics("assets/mystatic","myassets") // will be available at /myassets/*
	//sbus.App.LocalTemplates("assets/templates") // will make them available to use with c.Html

	// serve HTML 
	// sbus.App.Get("/",func(c *kmux.Context) {
	// 	c.Html("index.html", map[string]any{
	// 		"data": data,
	// 	})
	// })
	sbus.Run("localhost:9313")
}
```
Then create admin user to connect to the dashboard
```sh
go run main.go shell

createsuperuser
```

Then you can visit `/admin`

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

	
	bus := korm.WithBus(ksbus.NewServer())
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
	bus.App.GET("/",func(c *kmux.Context) {
		go bus.SendToServer("localhost:9314",map[string]any{
			"msg":"hello from server 1",
		})
		c.Status(200).Text("ok")
	})

	
	bus.Run("localhost:9313")
	// OR run https if you have certificates
	bus.RunTLS(addr string, cert string, certKey string)
	// OR generate certificates let's encrypt for a domain name, check https://github.com/kamalshkeir/ksbus for more details
	bus.RunAutoTLS(domainName string, subDomains ...string)
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

	
	bus := korm.WithBus(ksbus.NewServer())

	korm.BeforeServersData(func(data any, conn *ws.Conn) {
        klog.Printf("grrecv orm2: %v\n",data)
	})

	// built in router to the bus, check it at https://github.com/kamalshkeir/ksbus
	bus.App.GET("/",func(c *kmux.Context) {
		go bus.SendToServer("localhost:9314",map[string]any{
			"msg":"hello from server 2",
		})
		c.Status(200).Text("ok")
	})


    // Run Server Bus
	bus.Run("localhost:9314")

	// OR run https if you have certificates
	bus.RunTLS(addr string, cert string, certKey string)

	// OR generate certificates let's encrypt for a domain name, check https://github.com/kamalshkeir/ksbus for more infos
	bus.RunAutoTLS(domainName string, subDomains ...string)
}
```

# Hooks
```go
korm.OnInsert(func(database, table string, data map[string]any) error {
	fmt.Println("inserting into", database, table, data)
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

// korm.ManyToMany create relation table named m2m_table1_table2

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


# Benchmarks
```go
////////////////////////////////////////////    POSTGRES    //////////////////////////////////////////////
BenchmarkGetAllS_GORM-4            10000            106229 ns/op            5612 B/op        157 allocs/op
BenchmarkGetAllM_GORM-4             3036           5820141 ns/op         2094855 B/op      23046 allocs/op
BenchmarkGetRowS_GORM-4            10000            101521 ns/op            5940 B/op        133 allocs/op
BenchmarkGetRowM_GORM-4            10000            103402 ns/op            6392 B/op        165 allocs/op

BenchmarkGetAllS-4               3023593               385.7 ns/op           240 B/op          2 allocs/op
BenchmarkGetAllM-4               3767484               325.2 ns/op           240 B/op          2 allocs/op
BenchmarkGetRowS-4               2522994               480.2 ns/op           260 B/op          4 allocs/op
BenchmarkGetRowM-4               2711182               423.0 ns/op           260 B/op          4 allocs/op
BenchmarkGetAllTables-4         50003124                22.68 ns/op            0 B/op          0 allocs/op
BenchmarkGetAllColumns-4        24498944                47.71 ns/op            0 B/op          0 allocs/op

////////////////////////////////////////////    SQLITE      //////////////////////////////////////////////
BenchmarkGetAllS_GORM-4            12949             91299 ns/op            4171 B/op         95 allocs/op
BenchmarkGetAllM_GORM-4             3162           6063702 ns/op         2181614 B/op      23993 allocs/op
BenchmarkGetRowS_GORM-4            11848             95822 ns/op            5908 B/op        133 allocs/op
BenchmarkGetRowM_GORM-4            10000            103733 ns/op            6360 B/op        165 allocs/op

BenchmarkGetAllS-4               2982590               393.1 ns/op           240 B/op          2 allocs/op
BenchmarkGetAllM-4               3454128               334.3 ns/op           240 B/op          2 allocs/op
BenchmarkGetRowS-4               2406265               495.2 ns/op           260 B/op          4 allocs/op
BenchmarkGetRowM-4               2757932               437.2 ns/op           260 B/op          4 allocs/op
BenchmarkGetAllTables-4         51738410                22.68 ns/op            0 B/op          0 allocs/op
BenchmarkGetAllColumns-4        24481651                46.93 ns/op            0 B/op          0 allocs/op
////////////////////////////////////////////    MYSQL       //////////////////////////////////////////////
BenchmarkGetAllS-4               2933072               414.5 ns/op           208 B/op          2 allocs/op
BenchmarkGetAllM-4               6704588               180.4 ns/op            16 B/op          1 allocs/op
BenchmarkGetRowS-4               2136634               545.4 ns/op           240 B/op          4 allocs/op
BenchmarkGetRowM-4               4111814               292.6 ns/op            48 B/op          3 allocs/op
BenchmarkGetAllTables-4         58835394                21.52 ns/op            0 B/op          0 allocs/op
BenchmarkGetAllColumns-4        59059225                19.99 ns/op            0 B/op          0 allocs/op

////////////////////////////////////////////    MONGO       //////////////////////////////////////////////
BenchmarkGetAllS-4               2876449               409.8 ns/op           240 B/op          2 allocs/op
BenchmarkGetAllM-4               3431334               322.6 ns/op           240 B/op          2 allocs/op
BenchmarkGetRowS-4               2407183               506.7 ns/op           260 B/op          4 allocs/op
BenchmarkGetRowM-4               2690869               438.2 ns/op           260 B/op          4 allocs/op
BenchmarkGetAllTables-4         51621339                23.52 ns/op            0 B/op          0 allocs/op
//////////////////////////////////////////////////////////////////////////////////////////////////////////



package benchmarks

import (
	"testing"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/korm"
	"github.com/kamalshkeir/sqlitedriver"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type TestTable struct {
	Id      uint   `korm:"pk"`
	Content string `korm:"size:50"`
}

type TestTableGorm struct {
	ID      uint `gorm:"primarykey"`
	Content string
}

var gormDB *gorm.DB

func init() {
	var err error
	sqlitedriver.Use()
	gormDB, err = gorm.Open(sqlite.Open("benchgorm.sqlite"), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if klog.CheckError(err) {
		return
	}
	err = gormDB.AutoMigrate(&TestTableGorm{})
	if klog.CheckError(err) {
		return
	}
	dest := []TestTableGorm{}
	err = gormDB.Find(&dest,&TestTableGorm{}).Error
	if err != nil || len(dest) == 0 {
		err := gormDB.Create(&TestTableGorm{
			Content: "test",
		}).Error
		if klog.CheckError(err) {
			return
		}
	}
	_ = korm.New(korm.SQLITE, "bench")
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
}

func BenchmarkGetAllS_GORM(b *testing.B) {
	a := []TestTableGorm{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := gormDB.Find(&a).Error
		if err != nil {
			b.Error("error BenchmarkGetAllS_GORM:", err)
		}
	}
}

func BenchmarkGetAllM_GORM(b *testing.B) {
	a := []map[string]any{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := gormDB.Find(&TestTableGorm{}).Scan(&a).Error
		if err != nil {
			b.Error("error BenchmarkGetAllM_GORM:", err)
		}
	}
}

func BenchmarkGetRowS_GORM(b *testing.B) {
	u := TestTableGorm{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := gormDB.Where(&TestTableGorm{
			Content: "test",
		}).First(&u).Error
		if err != nil {
			b.Error("error BenchmarkGetRowS_GORM:", err)
		}
	}
}

func BenchmarkGetRowM_GORM(b *testing.B) {
	u := map[string]any{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := gormDB.Model(&TestTableGorm{}).Where(&TestTableGorm{
			Content: "test",
		}).First(&u).Error
		if err != nil {
			b.Error("error BenchmarkGetRowS_GORM:", err)
		}
	}
}

func BenchmarkGetAllS(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := korm.Model[TestTable]().All()
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