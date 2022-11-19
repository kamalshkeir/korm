package korm

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/kmux/ws"
	"github.com/kamalshkeir/ksbus"
)

var (
	// Debug when true show extra useful logs for queries executed for migrations and queries statements
	Debug = false
	// FlushCacheEvery execute korm.FlushCache() every 30 min by default, you should not worry about it, but useful that you can change it
	FlushCacheEvery = 30 * time.Minute
	// DefaultDB keep tracking of the first database connected
	DefaultDB         = ""
	useCache          = true
	databases         = []DatabaseEntity{}
	mModelTablename   = map[any]string{}
	cacheGetAllTables = kmap.New[string, []string](false)
	cachesOneM        = kmap.New[dbCache, map[string]any](false)
	cachesAllM        = kmap.New[dbCache, []map[string]any](false)

	onceDone = false
	cachebus *ksbus.Bus
)

const (
	MIGRATION_FOLDER = "migrations"
	CACHE_TOPIC      = "internal-db-cache"
	SQLITE           = "sqlite"
	POSTGRES         = "postgres"
	MYSQL            = "mysql"
	MARIA            = "maria"
	COCKROACH        = "cockroach"
)

type TableEntity struct {
	Pk         string
	Name       string
	Columns    []string
	Types      map[string]string
	ModelTypes map[string]string
	Tags       map[string][]string
}

type DatabaseEntity struct {
	Name      string
	Conn      *sql.DB
	Dialect   string
	Tables    []TableEntity
}

// NewDatabaseFromDSN the generic way to connect to all handled databases
func New(dbType, dbName string, dbDSN ...string) error {
	var dsn string
	if strings.HasPrefix(dbType, "cockroach") {
		dbType = POSTGRES
	}
	if DefaultDB == "" {
		DefaultDB = dbName
	}
	switch dbType {
	case POSTGRES:
		if len(dbDSN) == 0 {
			return errors.New("dbDSN for mysql cannot be empty")
		}
		dsn = fmt.Sprintf("postgres://%s/%s?sslmode=disable", dbDSN[0], dbName)
	case MYSQL:
		if len(dbDSN) == 0 {
			return errors.New("dbDSN for mysql cannot be empty")
		}
		if strings.Contains(dbDSN[0], "tcp(") {
			dsn = dbDSN[0] + "/" + dbName
		} else {
			split := strings.Split(dbDSN[0], "@")
			if len(split) > 2 {
				return errors.New("there is 2 or more @ symbol in dsn")
			}
			dsn = split[0] + "@" + "tcp(" + split[1] + ")/" + dbName
		}
	case MARIA, "mariadb":
		dbType = MARIA
		if len(dbDSN) == 0 {
			return errors.New("dbDSN for mysql cannot be empty")
		}
		if strings.Contains(dbDSN[0], "tcp(") {
			dsn = dbDSN[0] + "/" + dbName
		} else {
			split := strings.Split(dbDSN[0], "@")
			if len(split) > 2 {
				return errors.New("there is 2 or more @ symbol in dsn")
			}
			dsn = split[0] + "@" + "tcp(" + split[1] + ")/" + dbName
		}
	case SQLITE, "":
		dbType = SQLITE
		if dsn == "" {
			dsn = "db.sqlite"
		}
		if !strings.Contains(dbName, SQLITE) {
			dsn = dbName + ".sqlite"
		} else {
			dsn = dbName
		}
	default:
		klog.Printf("%s not handled, choices are: postgres,mysql,sqlite,maria,coakroach\n", dbType)
		dsn = dbName + ".sqlite"
		if dsn == "" {
			dsn = "db.sqlite"
		}
	}
	if dbType == SQLITE {
		dsn += "?_pragma=foreign_keys(1)"
	}

	if dbType == MARIA || dbType == "mariadb" {
		dbType = "mysql"
	}
	conn, err := sql.Open(dbType, dsn)
	if klog.CheckError(err) {
		return err
	}
	err = conn.Ping()
	if klog.CheckError(err) {
		klog.Printf("check if env is loaded %s \n", dsn)
		return err
	}
	dbFound := false
	for _, dbb := range databases {
		if dbb.Name == dbName {
			dbFound = true
		}
	}

	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(30 * time.Minute)
	conn.SetConnMaxIdleTime(12 * time.Hour)

	if !dbFound {
		databases = append(databases, DatabaseEntity{
			Name:    dbName,
			Conn:    conn,
			Dialect: dbType,
			Tables:  []TableEntity{},
		})
	}
	if !onceDone {
		if useCache {
			cachebus = ksbus.New()
			cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, ch ksbus.Channel) {
				handleCache(data)
			})
			go RunEvery(FlushCacheEvery, func() {
				cachebus.Publish(CACHE_TOPIC, map[string]any{
					"type": "clean",
				})
			})
		}
		runned := InitShell()
		if runned {
			os.Exit(0)
		}
		onceDone = true
	}

	return nil
}

// NewSQLDatabaseFromConnection register a new database from connection, without the need for a dsn, if you are already connected to it
func NewFromConnection(dbType, dbName string, conn *sql.DB) error {
	if strings.HasPrefix(dbType, "cockroach") {
		dbType = POSTGRES
	}
	if DefaultDB == "" {
		DefaultDB = dbName
	}
	err := conn.Ping()
	if klog.CheckError(err) {
		return err
	}
	dbFound := false
	for _, dbb := range databases {
		if dbb.Conn == conn && dbb.Name == dbName {
			if dbb.Dialect == "mariadb" {
				dbb.Dialect = MARIA
			}
			dbFound = true
		}
	}

	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(30 * time.Minute)
	conn.SetConnMaxIdleTime(10 * time.Second)
	if !dbFound {
		databases = append(databases, DatabaseEntity{
			Name:    dbName,
			Conn:    conn,
			Dialect: dbType,
			Tables:  []TableEntity{},
		})
	}
	if !onceDone {
		if useCache {
			cachebus = ksbus.New()
			cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, ch ksbus.Channel) { handleCache(data) })
			go RunEvery(FlushCacheEvery, func() {
				cachebus.Publish(CACHE_TOPIC, map[string]any{
					"type": "clean",
				})
			})
		}
		runned := InitShell()
		if runned {
			os.Exit(0)
		}
		onceDone = true
	}

	return nil
}

// WithBus take ksbus.NewServer() that can be Run, RunTLS, RunAutoTLS
func WithBus(bus *ksbus.Server) *ksbus.Server {
	cachebus = bus.Bus
	if useCache {
		cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, ch ksbus.Channel) { handleCache(data) })
		go RunEvery(FlushCacheEvery, func() {
			cachebus.Publish(CACHE_TOPIC, map[string]any{
				"type": "clean",
			})
		})
	}
	return bus
}

// BeforeServersData handle connections and data received from another server
func BeforeServersData(fn func(data any, conn *ws.Conn)) {
	ksbus.BeforeServersData = fn
}

// BeforeDataWS handle connections and data received before upgrading websockets, useful to handle authentication
func BeforeDataWS(fn func(data map[string]any, conn *ws.Conn, originalRequest *http.Request) bool) {
	ksbus.BeforeDataWS = fn
}

// FlushCache send msg to the cache system to Flush all the cache, safe to use in concurrent mode, and safe to use in general, it's done every 30 minutes(korm.FlushCacheEvery) and on update , create, delete , drop
func FlushCache() {
	go cachebus.Publish(CACHE_TOPIC, map[string]any{
		"type": "clean",
	})
}

// DisableCheck disable struct check for migrations on execution
func DisableCheck() {
	MigrationAutoCheck = false
}

// DisableCache disable the cache system, if you are having problem with it, you can korm.FlushCache on command too
func DisableCache() {
	useCache = false
}


func GetConnection(dbName ...string) (conn *sql.DB,ok bool) {
	var db *DatabaseEntity
	var err error
	if len(dbName) > 0 {
		db, err = GetMemoryDatabase(dbName[0])
	} else {
		db, err = GetMemoryDatabase(databases[0].Name)
	}
	if klog.CheckError(err) {
		return nil, false
	}
	if db.Conn != nil {
		conn = db.Conn
		return conn, true
	}
	return nil, false
}

// GetAllTables get all tables for the optional dbName given, otherwise, if not args, it will return tables of the first connected database
func GetAllTables(dbName ...string) []string {
	var name string
	if len(dbName) == 0 {
		name = databases[0].Name
	} else {
		name = dbName[0]
	}
	if useCache {
		if v, ok := cacheGetAllTables.Get(name); ok {
			return v
		}
	}
	tables := []string{}
	db,err := GetMemoryDatabase(name)
	if err == nil {
		for _, t := range db.Tables {
			tables = append(tables, t.Name)
		}
		if len(tables) > 0 {
			if useCache {
				cacheGetAllTables.Set(name, tables)
			}
			return tables
		}
	}

	conn,ok := GetConnection(name)
	if !ok {
		klog.Printf("rdconnection is null\n")
		return nil
	}

	switch db.Dialect {
	case POSTGRES:
		rows, err := conn.Query(`SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema','crdb_internal','pg_extension') AND tableowner != 'node'`)
		if klog.CheckError(err) {
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			var table string
			err := rows.Scan(&table)
			if klog.CheckError(err) {
				return nil
			}
			tables = append(tables, table)
		}
	case MYSQL, MARIA:
		rows, err := conn.Query("SELECT table_name,table_schema FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND table_schema ='" + name + "'")
		if klog.CheckError(err) {
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			var table string
			var table_schema string
			err := rows.Scan(&table, &table_schema)
			if klog.CheckError(err) {
				return nil
			}
			tables = append(tables, table)
		}
	case SQLITE, "":
		rows, err := conn.Query(`SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';`)
		if klog.CheckError(err) {
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			var table string
			err := rows.Scan(&table)
			if klog.CheckError(err) {
				return nil
			}
			tables = append(tables, table)
		}
	default:
		klog.Printf("rddatabase type not supported, should be sqlite, postgres, coakroach, maria or mysql")
		os.Exit(0)
	}
	if useCache {
		cacheGetAllTables.Set(name, tables)
	}
	return tables
}

// GetAllColumnsTypes get columns and types from the database
func GetAllColumnsTypes(table string, dbName ...string) map[string]string {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}

	tb, err := GetMemoryTable(table, dName)
	if err == nil {
		if len(tb.Types) > 0 {
			return tb.Types
		}
	}

	dbType := databases[0].Dialect
	conn,_ := GetConnection(dName)
	for _, d := range databases {
		if d.Name == dName {
			dbType = d.Dialect
			conn = d.Conn
		}
	}

	var statement string
	columns := map[string]string{}
	switch dbType {
	case POSTGRES:
		statement = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '" + table + "'"
	case MYSQL, MARIA:
		statement = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '" + table + "' AND TABLE_SCHEMA = '" + databases[0].Name + "'"
	default:
		statement = "PRAGMA table_info(" + table + ");"
		row, err := conn.Query(statement)
		if klog.CheckError(err) {
			return nil
		}
		defer row.Close()
		var num int
		var singleColName string
		var singleColType string
		var fake1 int
		var fake2 interface{}
		var fake3 int
		for row.Next() {
			err := row.Scan(&num, &singleColName, &singleColType, &fake1, &fake2, &fake3)
			if klog.CheckError(err) {
				return nil
			}
			columns[singleColName] = singleColType
		}
		return columns
	}

	row, err := conn.Query(statement)

	if klog.CheckError(err) {
		return nil
	}
	defer row.Close()
	var singleColName string
	var singleColType string
	for row.Next() {
		err := row.Scan(&singleColName, &singleColType)
		if klog.CheckError(err) {
			return nil
		}
		columns[singleColName] = singleColType
	}
	return columns
}

// GetMemoryTable get a table from memory for specified or first connected db
func GetMemoryTable(tbName string, dbName ...string) (TableEntity, error) {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}
	db, err := GetMemoryDatabase(dName)
	if err != nil {
		return TableEntity{}, err
	}
	for _, t := range db.Tables {
		if t.Name == tbName {
			return t, nil
		}
	}
	return TableEntity{}, errors.New("nothing found")
}

// GetMemoryTable get all tables from memory for specified or first connected db
func GetMemoryTables(dbName ...string) ([]TableEntity, error) {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}
	db, err := GetMemoryDatabase(dName)
	if err != nil {
		return nil, err
	}
	return db.Tables, nil
}

// GetMemoryDatabases get all databases from memory
func GetMemoryDatabases() []DatabaseEntity {
	return databases
}

// GetMemoryDatabase return the first connected database korm.DefaultDatabase if dbName "" or "default" else the matched db
func GetMemoryDatabase(dbName string) (*DatabaseEntity, error) {
	if DefaultDB == "" {
		DefaultDB = databases[0].Name
	}
	switch dbName {
	case "", "default":
		for i := range databases {
			if databases[i].Name == DefaultDB {
				return &databases[i], nil
			}
		}
		return nil, errors.New(dbName + "database not found")
	default:
		for i := range databases {
			if databases[i].Name == dbName {
				return &databases[i], nil
			}
		}
		return nil, errors.New(dbName + "database not found")
	}
}

// ShutdownDatabases shutdown many database
func ShutdownDatabases(databasesName ...string) error {
	if len(databasesName) > 0 {
		for _, db := range databases {
			if SliceContains(databasesName, db.Name) {
				if err := db.Conn.Close(); err != nil {
					return err
				}
			}
		}
		return nil
	} else {
		for i := range databases {
			if err := databases[i].Conn.Close(); err != nil {
				return err
			}
		}
		return nil
	}
}
