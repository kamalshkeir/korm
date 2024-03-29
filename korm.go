package korm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksbus"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/kstrct"
)

var (
	// defaultDB keep tracking of the first database connected
	defaultDB           = ""
	useCache            = true
	cacheMaxMemoryMb    = 100
	databases           = []DatabaseEntity{}
	mutexModelTablename sync.RWMutex
	mModelTablename     = map[string]any{}
	cacheAllTables      = kmap.New[string, []string](false)
	cacheAllCols        = kmap.New[string, map[string]string](false)
	cacheAllColsOrdered = kmap.New[string, []string](false)
	relationsMap        = kmap.New[string, struct{}](false)
	onceDone            = false
	serverBus           *ksbus.Server
	cacheQueryS         = kmap.New[dbCache, any](false, cacheMaxMemoryMb)
	cacheQueryM         = kmap.New[dbCache, any](false, cacheMaxMemoryMb)
	cacheQ              = kmap.New[string, any](false, cacheMaxMemoryMb)
	ErrTableNotFound    = errors.New("unable to find tableName")
	ErrBigData          = kmap.ErrLargeData
	logQueries          = false
)

// New the generic way to connect to all handled databases
//
//	Example:
//	  korm.New(korm.SQLITE, "db", sqlitedriver.Use())
//	  korm.New(korm.MYSQL,"dbName", mysqldriver.Use(), "user:password@localhost:3333")
//	  korm.New(korm.POSTGRES,"dbName", pgdriver.Use(), "user:password@localhost:5432")
func New(dbType Dialect, dbName string, dbDriver driver.Driver, dbDSN ...string) error {
	var dsn string
	if dbDriver == nil {
		klog.Printf("rdNew expect a dbDriver, you can use sqlitedriver.Use that return a driver.Driver \n")
		return fmt.Errorf("New expect a dbDriver, you can use sqlitedriver.Use that return a driver.Driver")
	}
	if defaultDB == "" {
		defaultDB = dbName
	}
	options := ""
	if len(dbDSN) > 0 {
		if strings.Contains(dbDSN[0], "?") {
			sp := strings.Split(dbDSN[0], "?")
			dbDSN[0] = sp[0]
			options = sp[1]
		}
	}
	switch dbType {
	case POSTGRES, COCKROACH:
		dbType = POSTGRES
		if len(dbDSN) == 0 {
			return errors.New("dbDSN for mysql cannot be empty")
		}
		dsn = "postgres://" + dbDSN[0] + "/" + dbName
		if options != "" {
			dsn += "?" + options
		} else {
			dsn += "?sslmode=disable"
		}
	case MYSQL, MARIA:
		dbType = MYSQL
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
		if options != "" {
			dsn += "?" + options
		}
	case SQLITE:
		if dsn == "" {
			dsn = "db.sqlite3"
		}
		if !strings.Contains(dbName, SQLITE) {
			dsn = dbName + ".sqlite3"
		} else {
			dsn = dbName
		}
		if options != "" {
			dsn += "?" + options
		} else {
			dsn += "?_foreign_keys=true"
		}
	default:
		dbType = "sqlite3"
		klog.Printf("%s not handled, choices are: postgres,mysql,sqlite3,maria,cockroach\n", dbType)
		dsn = dbName + ".sqlite3"
		if dsn == "" {
			dsn = "db.sqlite3"
		}
		if options != "" {
			dsn += "?" + options
		} else {
			dsn += "?_foreign_keys=true"
		}
	}

	cstm := GenerateUUID()
	if useCache {
		sql.Register(cstm, Wrap(dbDriver, &logAndCacheHook{}))
	} else {
		sql.Register(cstm, dbDriver)
	}

	conn, err := sql.Open(cstm, dsn)
	if klog.CheckError(err) {
		return err
	}
	err = conn.Ping()
	if klog.CheckError(err) {
		klog.Printf("check if env is loaded %s \n", dsn)
		return err
	}
	if dbType == SQLITE {
		conn.SetMaxOpenConns(1)
	} else {
		conn.SetMaxOpenConns(MaxOpenConns)
	}
	dbFound := false
	for _, dbb := range databases {
		if dbb.Name == dbName {
			dbFound = true
		}
	}

	conn.SetMaxIdleConns(MaxIdleConns)
	conn.SetConnMaxLifetime(MaxLifetime)
	conn.SetConnMaxIdleTime(MaxIdleTime)

	if !dbFound {
		databases = append(databases, DatabaseEntity{
			Name:    dbName,
			Conn:    conn,
			Dialect: dbType,
			Tables:  []TableEntity{},
		})
	}

	return nil
}

// WithShell enable shell, go run main.go shell
func WithShell() {
	runned := InitShell()
	if runned {
		os.Exit(0)
	}
}

// ManyToMany create m2m_table1_table2 many 2 many table
func ManyToMany(table1, table2 string, dbName ...string) error {
	var err error
	mdbName := databases[0].Name
	if len(dbName) > 0 {
		mdbName = dbName[0]
	}
	dben, err := GetMemoryDatabase(mdbName)
	if err != nil {
		return fmt.Errorf("database not found:%v", err)
	}

	fkeys := []string{}
	autoinc := ""

	defer func() {
		relationsMap.Set("m2m_"+table1+"-"+mdbName+"-"+table2, struct{}{})
	}()

	if _, ok := relationsMap.Get("m2m_" + table1 + "-" + mdbName + "-" + table2); ok {
		return nil
	}

	tables := GetAllTables(mdbName)
	if len(tables) == 0 {
		return fmt.Errorf("databse is empty: %v", tables)
	}
	for _, t := range tables {
		if t == table1+"_"+table2 || t == table2+"_"+table1 {
			return nil
		}
	}
	switch dben.Dialect {
	case SQLITE, "":
		autoinc = "INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"
	case POSTGRES, COCKROACH:
		autoinc = "SERIAL NOT NULL PRIMARY KEY"
	case MYSQL, MARIA:
		autoinc = "INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT"
	default:
		klog.Printf("dialect can be sqlite3, postgres, cockroach or mysql,maria only, not %s\n", dben.Dialect)
	}

	fkeys = append(fkeys, foreignkeyStat(table1+"_id", table1, "cascade", "cascade"))
	fkeys = append(fkeys, foreignkeyStat(table2+"_id", table2, "cascade", "cascade"))
	st := prepareCreateStatement(
		"m2m_"+table1+"_"+table2,
		map[string]string{
			"id":           autoinc,
			table1 + "_id": "INTEGER",
			table2 + "_id": "INTEGER",
		},
		fkeys,
		[]string{"id", table1 + "_id", table2 + "_id"},
	)
	if Debug {
		klog.Printfs("yl%s\n", st)
	}
	err = Exec(dben.Name, st)
	if err != nil {
		return err
	}
	dben.Tables = append(dben.Tables, TableEntity{
		Types: map[string]string{
			"id":           "uint",
			table1 + "_id": "uint",
			table2 + "_id": "uint",
		},
		Columns: []string{"id", table1 + "_id", table2 + "_id"},
		Name:    "m2m_" + table1 + "_" + table2,
		Pk:      "id",
	})
	return nil
}

// WithBus return ksbus.NewServer() that can be Run, RunTLS, RunAutoTLS
func WithBus() *ksbus.Server {
	if serverBus == nil {
		serverBus = ksbus.NewServer()
	}
	return serverBus
}

// WithDashboard enable admin dashboard
func WithDashboard(staticAndTemplatesEmbeded ...embed.FS) *ksbus.Server {
	EmbededDashboard = len(staticAndTemplatesEmbeded) > 0
	if serverBus == nil {
		serverBus = WithBus()
	}
	klog.UsePublisher(serverBus, "klog:logs")
	cloneAndMigrateDashboard(true, staticAndTemplatesEmbeded...)
	initAdminUrlPatterns(serverBus.App)
	var razor = `
                               __
  .'|   .'|   .'|=|'.     .'|=|  |   .'|\/|'.
.'  | .' .' .'  | |  '. .'  | |  | .'  |  |  '.
|   |=|.:   |   | |   | |   |=|.'  |   |  |   |
|   |   |'. '.  | |  .' |   |  |'. |   |  |   |
|___|   |_|   '.|=|.'   |___|  |_| |___|  |___|
`
	klog.Printfs("yl%s\n", razor)
	return serverBus
}

// WithDocs enable swagger docs at DocsUrl default to '/docs/'
func WithDocs(generateJsonDocs bool, outJsonDocs string, handlerMiddlewares ...func(handler ksmux.Handler) ksmux.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus()
	}

	if outJsonDocs != "" {
		ksmux.DocsOutJson = outJsonDocs
	} else {
		ksmux.DocsOutJson = StaticDir + "/docs"
	}

	IsDocsUsed = true
	// check swag install and init docs.Routes slice
	serverBus.App.WithDocs(generateJsonDocs)
	webPath := DocsUrl
	if webPath[0] != '/' {
		webPath = "/" + webPath
	}
	webPath = strings.TrimSuffix(webPath, "/")
	handler := func(c *ksmux.Context) {
		http.StripPrefix(webPath, http.FileServer(http.Dir(ksmux.DocsOutJson))).ServeHTTP(c.ResponseWriter, c.Request)
	}
	if len(handlerMiddlewares) > 0 {
		for _, mid := range handlerMiddlewares {
			handler = mid(handler)
		}
	}
	serverBus.App.Get(webPath+"/*path", handler)
	return serverBus
}

// WithEmbededDocs same as WithDocs but embeded, enable swagger docs at DocsUrl default to '/docs/'
func WithEmbededDocs(embeded embed.FS, embededDirPath string, handlerMiddlewares ...func(handler ksmux.Handler) ksmux.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus()
	}
	if embededDirPath != "" {
		ksmux.DocsOutJson = embededDirPath
	} else {
		ksmux.DocsOutJson = StaticDir + "/docs"
	}
	webPath := DocsUrl

	ksmux.DocsOutJson = filepath.ToSlash(ksmux.DocsOutJson)
	if webPath[0] != '/' {
		webPath = "/" + webPath
	}
	webPath = strings.TrimSuffix(webPath, "/")
	toembed_dir, err := fs.Sub(embeded, ksmux.DocsOutJson)
	if err != nil {
		klog.Printf("rdServeEmbededDir error= %v\n", err)
		return serverBus
	}
	toembed_root := http.FileServer(http.FS(toembed_dir))
	handler := func(c *ksmux.Context) {
		http.StripPrefix(webPath, toembed_root).ServeHTTP(c.ResponseWriter, c.Request)
	}
	if len(handlerMiddlewares) > 0 {
		for _, mid := range handlerMiddlewares {
			handler = mid(handler)
		}
	}
	serverBus.App.Get(webPath+"/*path", handler)
	return serverBus
}

// WithMetrics enable path /metrics (default), it take http.Handler like promhttp.Handler()
func WithMetrics(httpHandler http.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus()
	}
	serverBus.WithMetrics(httpHandler)
	return serverBus
}

// WithPprof enable std library pprof at /debug/pprof, prefix default to 'debug'
func WithPprof(path ...string) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus()
	}
	serverBus.WithPprof(path...)
	return serverBus
}

// Transaction create new database/sql transaction and return it, it can be rollback ...
func Transaction(dbName ...string) (*sql.Tx, error) {
	return GetConnection(dbName...).Begin()
}

// FlushCache send msg to the cache system to Flush all the cache, safe to use in concurrent mode, and safe to use in general, it's done every 30 minutes(korm.FlushCacheEvery) and on update , create, delete , drop
func FlushCache() {
	flushCache()
}

// DisableCache disable the cache system, if and only if you are having problem with it, also you can korm.FlushCache on command too
func DisableCache() {
	useCache = false
}

// GetConnection get connection of dbName, if not specified , it return default, first database connected
func GetConnection(dbName ...string) *sql.DB {
	var name string
	var db *DatabaseEntity
	if len(dbName) > 0 {
		var err error
		db, err = GetMemoryDatabase(dbName[0])
		if klog.CheckError(err) {
			return nil
		}
	} else {
		name = databases[0].Name
		db = &databases[0]
	}

	if db.Conn == nil {
		klog.Printf("rdmemory database %s have no connection\n", name)
	}
	return db.Conn
}

// GetAllTables get all tables for the optional dbName given, otherwise, if not args, it will return tables of the first connected database
func GetAllTables(dbName ...string) []string {
	var name string
	if len(dbName) == 0 {
		name = databases[0].Name
	} else {
		name = dbName[0]
	}
	db, err := GetMemoryDatabase(name)
	if err != nil {
		return nil
	}
	if useCache {
		if v, ok := cacheAllTables.Get(name); ok {
			if len(v) == len(db.Tables) {
				return v
			}
		}
	}

	tables := []string{}

	switch db.Dialect {
	case POSTGRES:
		rows, err := db.Conn.Query(`select tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema','crdb_internal','pg_extension') AND tableowner != 'node'`)
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
		rows, err := db.Conn.Query("SELECT table_name,table_schema FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND table_schema ='" + name + "'")
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
		rows, err := db.Conn.Query(`select name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';`)
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
		klog.Printf("rddatabase type not supported, should be sqlite3, postgres, cockroach, maria or mysql")
		return nil
	}
	if useCache && len(tables) > 0 {
		cacheAllTables.Set(name, tables)
	}

	return tables
}

// GetAllColumnsTypes get columns and types from the database
func GetAllColumnsTypes(table string, dbName ...string) (map[string]string, []string) {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}
	if useCache {
		if v, ok := cacheAllCols.Get(dName + table); ok {
			if vv, ok := cacheAllColsOrdered.Get(dName + table); ok {
				return v, vv
			}
		}
	}

	db, err := GetMemoryDatabase(dName)
	if err != nil {
		return nil, nil
	}

	var statement string
	colsSlice := []string{}
	columns := map[string]string{}
	switch db.Dialect {
	case POSTGRES:
		statement = "select column_name,data_type FROM information_schema.columns WHERE table_name = '" + table + "'"
	case MYSQL, MARIA:
		statement = "select column_name,data_type FROM information_schema.columns WHERE table_name = '" + table + "' AND TABLE_SCHEMA = '" + db.Name + "'"
	default:
		statement = "pragma table_info(" + table + ");"
		row, err := db.Conn.Query(statement)
		if klog.CheckError(err) {
			return nil, nil
		}
		defer row.Close()
		var num int
		var singleColName string
		var singleColType string
		var fake1 int
		var fake2 any
		var fake3 int
		for row.Next() {
			err := row.Scan(&num, &singleColName, &singleColType, &fake1, &fake2, &fake3)
			if klog.CheckError(err) {
				return nil, nil
			}
			columns[singleColName] = singleColType
			colsSlice = append(colsSlice, singleColName)
		}
		if useCache {
			cacheAllCols.Set(dName+table, columns)
		}
		return columns, colsSlice
	}

	row, err := db.Conn.Query(statement)

	if klog.CheckError(err) {
		return nil, nil
	}
	defer row.Close()
	var singleColName string
	var singleColType string
	for row.Next() {
		err := row.Scan(&singleColName, &singleColType)
		if klog.CheckError(err) {
			return nil, nil
		}
		columns[singleColName] = singleColType
	}
	if useCache {
		cacheAllCols.Set(dName+table, columns)
		cacheAllColsOrdered.Set(dName+table, colsSlice)
	}
	return columns, colsSlice
}

// Shutdown shutdown many database
func Shutdown(dbNames ...string) error {
	if len(dbNames) > 0 {
		for _, db := range databases {
			if SliceContains(dbNames, db.Name) {
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

// Exec exec sql and return error if any
func Exec(dbName, query string, args ...any) error {
	conn := GetConnection(dbName)
	if conn == nil {
		return errors.New("no connection found")
	}
	adaptTimeToUnixArgs(&args)
	_, err := conn.Exec(query, args...)
	if err != nil {
		return err
	}
	return nil
}

// ExecContext exec sql and return error if any
func ExecContext(ctx context.Context, dbName, query string, args ...any) error {
	conn := GetConnection(dbName)
	if conn == nil {
		return errors.New("no connection found")
	}
	adaptTimeToUnixArgs(&args)
	_, err := conn.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return nil
}

// ExecNamed exec named sql and return error if any
func ExecNamed(query string, args map[string]any, dbName ...string) error {
	db := databases[0]
	if len(dbName) > 0 && dbName[0] != "" {
		dbb, err := GetMemoryDatabase(dbName[0])
		if err != nil {
			return errors.New("no connection found")
		}
		db = *dbb
	}
	q, newargs, err := AdaptNamedParams(db.Dialect, query, args)
	if err != nil {
		return err
	}
	_, err = db.Conn.Exec(q, newargs...)
	if err != nil {
		return err
	}
	return nil
}

// ExecContextNamed exec named sql and return error if any
func ExecContextNamed(ctx context.Context, query string, args map[string]any, dbName ...string) error {
	db := databases[0]
	if len(dbName) > 0 && dbName[0] != "" {
		dbb, err := GetMemoryDatabase(dbName[0])
		if err != nil {
			return errors.New("no connection found")
		}
		db = *dbb
	}
	q, newargs, err := AdaptNamedParams(db.Dialect, query, args)
	if err != nil {
		return err
	}
	_, err = db.Conn.ExecContext(ctx, q, newargs...)
	if err != nil {
		return err
	}
	return nil
}

type KV kstrct.KV

func getTableName[T any]() string {
	mutexModelTablename.RLock()
	defer mutexModelTablename.RUnlock()
	for k, v := range mModelTablename {
		if _, ok := v.(T); ok {
			return k
		} else if _, ok := v.(*T); ok {
			return k
		}
	}
	return ""
}

// LogQueries enable logging sql statements with time tooked
func LogQueries() {
	logQueries = true
}
