<div align="center">
	<h1 style="color:black;font-size:clamp(30px,12vw,100px);padding-bottom:0;">KORM</h1>
	<h3 style="color:#dddd00;font-size:clamp(20px,4vw,40px);">
	<a href="#benchmarks" style="text-decoration:none;color:#dddd00">The Blazingly Fast ORM</a></h3>
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
### Let [numbers](#benchmarks) speak for themselves
## KORM is an Elegant and dead easy to use [ORM](#api-working-with-sql-and-mongo) using generics and network bus, that can handle sql and mongo dbs. 
## It is Composable, you can use it as ORM with internal bus only , or add the Server Bus to it when you want to scale and synchronise your data between multiple KORM, you have full control on the data came in and go out, also the Bus come with built-in , you can check the bus via this [link](https://github.com/kamalshkeir/ksbus)


#### It come with :
- [Interactive Shell](#interactive-shell), to CRUD in your databases `go run main.go dbshell`
- Network Bus allowing you to send and recv data in realtime using pubsub websockets between your ORMs, so you can decide how you data will be distributed between different databases, see [Example](#example-with-bus-between-2-korm) .
- [AutoMigrate](#automigrate) directly from struct, whenever you add or remove a field from a migrated struct, you will get a prompt proposing to add the column for the table in the database or remove a column, you can also only generate the query without execute, and then you can use the shell to migrate the generated file.
- It use std library database/sql, and the Mongo official driver, so if you want, know that you can always do your queries yourself using sql.DB or mongo.Client , but i doubt you will need to, after seeing these [benchmarks](#benchmarks) . `korm.GetConnection(dbName)` AND `korm.GetMONGOConnection(dbName) and korm.GetMONGOClient()`
- Powerful Query Builder for SQL and Mongo [Builder](#builder-mapstringany).
- Support of foreign keys, indexes , checks,... [See all](#automigrate)
- It handle query and execute on multiple database at the same time.
- Concurrency Safe.


#### Supported databases:
- Postgres
- Mysql
- Mongo
- Sqlite
- Maria
- Coakroach



---
# Installation

```sh
go get -u github.com/kamalshkeir/korm@v1.2.0
```

### Connect to a database
```go
// mongodb
err := korm.NewDatabaseFromDSN(korm.MONGO, "dbmongo", "localhost:27017")
// sqlite
err := korm.NewDatabaseFromDSN(korm.SQLITE, "db")
// postgres
err := korm.NewDatabaseFromDSN(korm.POSTGRES,"dbName", "localhost:5432")
// mysql
err := korm.NewDatabaseFromDSN(korm.MYSQL,"dbName","localhost:3306")
...
...
```

### AutoMigrate 

[Available Tags](#available-tags-by-struct-field-type) (SQL)

SQL:
```go
korm.AutoMigrate[T comparable](tableName string, dbName ...string) error 

err := korm.AutoMigrate[User]("users")
err := korm.AutoMigrate[Bookmark ]("bookmarks")
//Examples:
// this is the actual user model used initialy
type User struct {
	Id        int       `korm:"pk"` // AUTO Increment ID primary key
	Uuid      string    `korm:"size:40"` // VARCHAR(50)
	Email     string    `korm:"size:50;iunique"` // insensitive unique
	Password  string    `korm:"size:150"`
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
                   .Where("_id = ?",id)
                   .Select("item1","item2")
                   .OrderBy("created")
                   .All()
```

### API (working with SQL and MONGO)
#### General
```go
func NewDatabaseFromDSN(dbType, dbName string, dbDSN ...string) error
func NewSQLDatabaseFromConnection(dbType, dbName string, conn *sql.DB) error
func NewBusServerKORM() *ksbus.Server
func BeforeServersData(fn func(data any, conn *ws.Conn))
func BeforeDataWS(fn func(data map[string]any, conn *ws.Conn, originalRequest *http.Request) bool)
func FlushCache()
func DisableCheck()
func DisableCache()
func GetSQLConnection(dbName ...string) *sql.DB
func GetMONGOConnection(dbName ...string) *mongo.Database
func GetMONGOClient(dbName ...string) *mongo.Client
func GetAllTables(dbName ...string) []string
func GetAllColumnsTypes(table string, dbName ...string) map[string]string
func GetMemoryTable(tbName string, dbName ...string) (TableEntity, error)
func GetMemoryTables(dbName ...string) ([]TableEntity, error)
func GetMemoryDatabases() []DatabaseEntity
func GetMemoryDatabase(dbName string) (*DatabaseEntity, error)
```

#### Builder `map[string]any`:
```go
func Table(tableName string) *BuilderM // starter
func BuilderMap(tableName string) *BuilderM // starter
func (b *BuilderM) Database(dbName string) *BuilderM // select database
func (b *BuilderM) Select(columns ...string) *BuilderM // select columns
func (b *BuilderM) Where(query string, args ...any) *BuilderM
func (b *BuilderM) Query(query string, args ...any) *BuilderM
func (b *BuilderM) Limit(limit int) *BuilderM
func (b *BuilderM) Page(pageNumber int) *BuilderM
func (b *BuilderM) OrderBy(fields ...string) *BuilderM
func (b *BuilderM) Context(ctx context.Context) *BuilderM
func (b *BuilderM) Debug() *BuilderM // show executed queries for migrations
func (b *BuilderM) All() ([]map[string]any, error) // finisher
func (b *BuilderM) One() (map[string]any, error) // finisher
func (b *BuilderM) Insert(fields_comma_separated string, fields_values ...any) (int, error) // finisher
func (b *BuilderM) Set(query string, args ...any) (int, error) // finisher
func (b *BuilderM) Delete() (int, error) // finisher
func (b *BuilderM) Drop() (int, error) // finisher
func Query(dbName string, statement string, args ...any) ([]map[string]interface{}, error) // finisher
func ExecSQL(dbName, query string, args ...any) error

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
korm.Model[models.User]().Insert(
	"uuid,email,password,is_admin,image,created_at",
	uuid,
	"test@example.com",
	hashedPass,
	false,
	"",
	time.Now())

//if using more than one db
korm.Database("dbNameHere").Table("tableName").Where("id = ? AND email = ?",1,"test@example.com").All() 

// where
korm.Table("tableName").Where("id = ? AND email = ?",1,"test@example.com").One() 

// delete
korm.Table("tableName").Where("id = ? AND email = ?",1,"test@example.com").Delete()

// drop table
korm.Table("tableName").Drop()

// update
korm.Table("tableName").Where("id = ?",1).Set("email = ?","new@example.com")
```
#### Builder `Struct`:
```go
func Model[T comparable](tableName ...string) *Builder[T] // you get the idea ;) 
func BuilderS[T comparable](tableName ...string) *Builder[T] 
func (b *Builder[T]) Database(dbName string) *Builder[T]
func (b *Builder[T]) Insert(model *T) (int, error)
func (b *Builder[T]) Set(query string, args ...any) (int, error)
func (b *Builder[T]) Delete() (int, error)
func (b *Builder[T]) Drop() (int, error)
func (b *Builder[T]) Select(columns ...string) *Builder[T]
func (b *Builder[T]) Where(query string, args ...any) *Builder[T]
func (b *Builder[T]) Query(query string, args ...any) *Builder[T]
func (b *Builder[T]) Limit(limit int) *Builder[T]
func (b *Builder[T]) Context(ctx context.Context) *Builder[T]
func (b *Builder[T]) Page(pageNumber int) *Builder[T]
func (b *Builder[T]) OrderBy(fields ...string) *Builder[T]
func (b *Builder[T]) Debug() *Builder[T]
func (b *Builder[T]) All() ([]T, error)
func (b *Builder[T]) One() (T, error)

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
)

func main() {
	err := korm.NewDatabaseFromDSN(korm.SQLITE,"db1")
	if klog.CheckError(err) {return}

	
	bus := korm.NewBusServerKORM()
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
	// OR generate certificates let's encrypt for a domain name, check https://github.com/kamalshkeir/ksbus for more infos
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
	err := korm.NewDatabaseFromDSN(korm.SQLITE,"db2")
	if klog.CheckError(err) {return}

	
	bus := korm.NewBusServerKORM()

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



type TestTable struct {
	Id      uint   `korm:"pk"`
	Content string `korm:"size:50"`
}

type TestTable struct {
    gorm.Model
	Content string 
}

gormDB, err = gorm.Open(sqlite.Open("bench.sqlite"), &gorm.Config{
    SkipDefaultTransaction: true,
})
klog.CheckError(err)
err = gormDB.AutoMigrate(&TestTable{})
err = gormDB.Create(&TestTable{
    Content: "test",
}).Error
klog.CheckError(err)

_ = korm.NewDatabaseFromDSN(korm.SQLITE,"bench")
// migrate table test_table from struct TestTable
err := korm.AutoMigrate[TestTable]("test_table")
if klog.CheckError(err){
	return
}
t, _ := korm.Table("test_table").All()
if len(t) == 0 {
	_,err := korm.Model[TestTable]().Insert(&TestTable{
		Content: "test",
	})
	klog.CheckError(err)
}


func BenchmarkGetAllS_GORM(b *testing.B) {
	a := []TestTable{}
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
		err := gormDB.Find(&TestTable{}).Scan(&a).Error
		if err != nil {
			b.Error("error BenchmarkGetAllM_GORM:", err)
		}
	}
}

func BenchmarkGetRowS_GORM(b *testing.B) {
	u := TestTable{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := gormDB.Where(&TestTable{
			Content: "dqsdq",
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
		err := gormDB.Model(&TestTable{}).Where(&TestTable{
			Content: "dqsdq",
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
[databases, use, tables, columns, migrate, 
getall, get, drop, delete, clear/cls, q/quit/exit, help/commands]
  'databases':
	  list all connected databases

  'use':
	  use a specific database

  'tables':
	  list all tables in database

  'columns':
	  list all columns of a table

  'migrate':
	  migrate sql file

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