package korm

import (
	"database/sql"
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
	"github.com/kamalshkeir/kmux"
	"github.com/kamalshkeir/ksbus"
	"github.com/kamalshkeir/kstrct"
)

var (
	// defaultDB keep tracking of the first database connected
	defaultDB           = ""
	useCache            = true
	cacheMaxMemoryMb    = 100
	databases           = []DatabaseEntity{}
	mutexModelTablename sync.RWMutex
	mModelTablename     = map[any]string{}
	cacheAllTables      = kmap.New[string, []string](false)
	cacheAllCols        = kmap.New[string, map[string]string](false)
	relationsMap        = kmap.New[string, struct{}](false)
	onceDone            = false
	serverBus           *ksbus.Server
	cachebus            *ksbus.Bus
	switchBusMutex      sync.Mutex
	cacheQueryS         = kmap.New[dbCache, any](false, cacheMaxMemoryMb)
	cacheQueryM         = kmap.New[dbCache, any](false, cacheMaxMemoryMb)
	ErrTableNotFound    = errors.New("unable to find tableName")
	ErrBigData          = kmap.ErrLargeData
)

// NewDatabaseFromDSN the generic way to connect to all handled databases
func New(dbType Dialect, dbName string, dbDSN ...string) error {
	var dsn string
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
		dbType = "sqlite3"
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

	conn, err := sql.Open(string(dbType), dsn)
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

	conn.SetMaxOpenConns(MaxOpenConns)
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
	if !onceDone {
		if useCache {
			cachebus = ksbus.New()
			cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, _ ksbus.Channel) {
				handleCache(data)
			})
			go RunEvery(FlushCacheEvery, func() {
				switchBusMutex.Lock()
				defer switchBusMutex.Unlock()
				cachebus.Publish(CACHE_TOPIC, map[string]any{
					"type": "clean",
				})
			})
		}
		onceDone = true
	}

	return nil
}

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

// NewSQLDatabaseFromConnection register a new database from connection, without the need for a dsn, if you are already connected to it
func NewFromConnection(dbType, dbName string, conn *sql.DB) error {
	if strings.HasPrefix(dbType, "cockroach") {
		dbType = POSTGRES
	}
	if dbType == MARIA {
		dbType = MYSQL
	}
	if defaultDB == "" {
		defaultDB = dbName
	}

	err := conn.Ping()
	if err != nil {
		klog.Printf("rdcouldn't ping database %s : %v\n", dbName, err)
		return err
	}
	dbFound := false
	for _, dbb := range databases {
		if dbb.Name == dbName {
			dbFound = true
		}
	}

	conn.SetMaxOpenConns(MaxOpenConns)
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
	if !onceDone {
		if useCache {
			cachebus = ksbus.New()
			cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, _ ksbus.Channel) {
				handleCache(data)
			})
			go RunEvery(FlushCacheEvery, func() {
				switchBusMutex.Lock()
				defer switchBusMutex.Unlock()
				cachebus.Publish(CACHE_TOPIC, map[string]any{
					"type": "clean",
				})
			})
		}
		onceDone = true
	}

	return nil
}

// WithBus take ksbus.NewServer() that can be Run, RunTLS, RunAutoTLS
func WithBus(bus *ksbus.Server) *ksbus.Server {
	switchBusMutex.Lock()
	serverBus = bus
	cachebus = bus.Bus
	switchBusMutex.Unlock()
	if useCache {
		cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, _ ksbus.Channel) { handleCache(data) })
		go RunEvery(FlushCacheEvery, func() {
			cachebus.Publish(CACHE_TOPIC, map[string]any{
				"type": "clean",
			})
		})
	}
	return bus
}

func WithDashboard(staticAndTemplatesEmbeded ...embed.FS) *ksbus.Server {
	EmbededDashboard = len(staticAndTemplatesEmbeded) > 0
	if serverBus == nil {
		serverBus = WithBus(ksbus.NewServer())
	}
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

func WithDocs(generateJsonDocs bool, outJsonDocs string, handlerMiddlewares ...func(handler kmux.Handler) kmux.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus(ksbus.NewServer())
	}

	if outJsonDocs != "" {
		kmux.DocsOutJson = outJsonDocs
	} else {
		kmux.DocsOutJson = StaticDir + "/docs"
	}

	docsUsed = true
	// check swag install and init docs.Routes slice
	serverBus.App.WithDocs(generateJsonDocs)
	webPath := DocsUrl
	if webPath[0] != '/' {
		webPath = "/" + webPath
	}
	webPath = strings.TrimSuffix(webPath, "/")
	handler := func(c *kmux.Context) {
		http.StripPrefix(webPath, http.FileServer(http.Dir(kmux.DocsOutJson))).ServeHTTP(c.ResponseWriter, c.Request)
	}
	if len(handlerMiddlewares) > 0 {
		for _, mid := range handlerMiddlewares {
			handler = mid(handler)
		}
	}
	serverBus.App.GET(webPath+"/*path", handler)
	return serverBus
}

func WithEmbededDocs(embeded embed.FS, embededDirPath string, handlerMiddlewares ...func(handler kmux.Handler) kmux.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus(ksbus.NewServer())
	}
	if embededDirPath != "" {
		kmux.DocsOutJson = embededDirPath
	} else {
		kmux.DocsOutJson = StaticDir + "/docs"
	}
	webPath := DocsUrl

	kmux.DocsOutJson = filepath.ToSlash(kmux.DocsOutJson)
	if webPath[0] != '/' {
		webPath = "/" + webPath
	}
	webPath = strings.TrimSuffix(webPath, "/")
	toembed_dir, err := fs.Sub(embeded, kmux.DocsOutJson)
	if err != nil {
		klog.Printf("rdServeEmbededDir error= %v\n", err)
		return serverBus
	}
	toembed_root := http.FileServer(http.FS(toembed_dir))
	handler := func(c *kmux.Context) {
		http.StripPrefix(webPath, toembed_root).ServeHTTP(c.ResponseWriter, c.Request)
	}
	if len(handlerMiddlewares) > 0 {
		for _, mid := range handlerMiddlewares {
			handler = mid(handler)
		}
	}
	serverBus.App.GET(webPath+"/*path", handler)
	return serverBus
}

// WithMetrics enable path /metrics (default), it take http.Handler like promhttp.Handler()
func WithMetrics(httpHandler http.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus(ksbus.NewServer())
	}
	serverBus.WithMetrics(httpHandler)
	return serverBus
}

// WithPprof enable std library pprof at /debug/pprof, prefix default to 'debug'
func WithPprof(path ...string) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus(ksbus.NewServer())
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
	cachebus.Publish(CACHE_TOPIC, map[string]any{
		"type": "clean",
	})
}

// DisableCheck disable struct check for migrations on execution
func DisableCheck() {
	migrationAutoCheck = false
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
		rows, err := db.Conn.Query(`SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema','crdb_internal','pg_extension') AND tableowner != 'node'`)
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
		rows, err := db.Conn.Query(`SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';`)
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
func GetAllColumnsTypes(table string, dbName ...string) map[string]string {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}
	if useCache {
		if v, ok := cacheAllCols.Get(dName + table); ok {
			return v
		}
	}

	db, err := GetMemoryDatabase(dName)
	if err != nil {
		return nil
	}

	var statement string
	columns := map[string]string{}
	switch db.Dialect {
	case POSTGRES:
		statement = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '" + table + "'"
	case MYSQL, MARIA:
		statement = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '" + table + "' AND TABLE_SCHEMA = '" + db.Name + "'"
	default:
		statement = "PRAGMA table_info(" + table + ");"
		row, err := db.Conn.Query(statement)
		if klog.CheckError(err) {
			return nil
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
				return nil
			}
			columns[singleColName] = singleColType
		}
		if useCache {
			cacheAllCols.Set(dName+table, columns)
		}
		return columns
	}

	row, err := db.Conn.Query(statement)

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
	if useCache {
		cacheAllCols.Set(dName+table, columns)
	}
	return columns
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

// Query query sql and return result as slice maps
func Query(dbName string, statement string, args ...any) ([]map[string]any, error) {
	if dbName == "" {
		dbName = databases[0].Name
	}
	c := dbCache{
		database:  dbName,
		statement: statement,
		args:      fmt.Sprint(args...),
	}
	if useCache {
		if v, ok := cacheQueryM.Get(c); ok {
			return v.([]map[string]any), nil
		}
	}
	db, err := GetMemoryDatabase(dbName)
	if err != nil {
		return nil, err
	}
	if db.Conn == nil {
		return nil, errors.New("no connection")
	}
	adaptPlaceholdersToDialect(&statement, db.Dialect)
	adaptTrueFalseArgs(&args)
	var rows *sql.Rows
	rows, err = db.Conn.Query(statement, args...)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("queryM: no data found")
	} else if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	models := make([]any, len(columns))
	modelsPtrs := make([]any, len(columns))

	listMap := make([]map[string]any, 0)

	for rows.Next() {
		for i := range models {
			models[i] = &modelsPtrs[i]
		}

		err := rows.Scan(models...)
		if err != nil {
			return nil, err
		}

		m := map[string]any{}
		for i := range columns {
			if db.Dialect == MYSQL || db.Dialect == MARIA {
				if v, ok := modelsPtrs[i].([]byte); ok {
					modelsPtrs[i] = string(v)
				}
			}
			m[columns[i]] = modelsPtrs[i]
		}
		listMap = append(listMap, m)
	}
	if len(listMap) == 0 {
		return nil, errors.New("no data found")
	}
	if useCache {
		_ = cacheQueryM.Set(c, listMap)
	}
	return listMap, nil
}

// Exec exec sql and return error if any
func Exec(dbName, query string, args ...any) error {
	conn := GetConnection(dbName)
	if conn == nil {
		return errors.New("no connection found")
	}
	adaptTrueFalseArgs(&args)
	_, err := conn.Exec(query, args...)
	if err != nil {
		return err
	}
	return nil
}

func getTableName[T comparable]() string {
	mutexModelTablename.RLock()
	defer mutexModelTablename.RUnlock()
	if v, ok := mModelTablename[*new(T)]; ok {
		return v
	} else {
		return ""
	}
}

func QueryS[T any](dbName string, statement string, args ...any) ([]T, error) {
	if dbName == "" {
		dbName = databases[0].Name
	}
	c := dbCache{
		database:  dbName,
		statement: statement,
		args:      fmt.Sprint(args...),
	}
	if useCache {
		if v, ok := cacheQueryS.Get(c); ok {
			return v.([]T), nil
		}
	}
	db, err := GetMemoryDatabase(dbName)
	if err != nil {
		return nil, err
	}
	if db.Conn == nil {
		return nil, errors.New("no connection")
	}
	adaptPlaceholdersToDialect(&statement, db.Dialect)
	adaptTrueFalseArgs(&args)

	rows, err := db.Conn.Query(statement, args...)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no data found")
	} else if err != nil {
		return nil, err
	}

	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columns_ptr_to_values := make([]any, len(columns))
	values := make([]any, len(columns))
	res := make([]T, 0)

	for rows.Next() {
		for i := range values {
			columns_ptr_to_values[i] = &values[i]
		}

		err := rows.Scan(columns_ptr_to_values...)
		if err != nil {
			klog.Printf("yl%s\n", statement)
			return nil, err
		}

		row := new(T)

		m := map[string]any{}
		for i, key := range columns {
			if db.Dialect == MYSQL || db.Dialect == MARIA {
				if v, ok := values[i].([]byte); ok {
					values[i] = string(v)
				}
			}
			m[key] = values[i]
		}
		err = kstrct.FillFromMap(row, m)
		if err != nil {
			return nil, err
		}
		res = append(res, *row)
	}

	if len(res) == 0 {
		return nil, errors.New("no data found")
	}
	if useCache {
		_ = cacheQueryS.Set(c, res)
	}
	return res, nil
}
