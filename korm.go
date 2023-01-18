package korm

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/kmux/ws"
	"github.com/kamalshkeir/ksbus"
)

var (
	// defaultDB keep tracking of the first database connected
	defaultDB           = ""
	useCache            = true
	cacheMaxMemoryMb    = 50
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

	ErrTableNotFound = errors.New("unable to find tableName")
	ErrBigData       = kmap.ErrLargeData
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
		if dsn == "" {
			dsn = "db.sqlite"
		}
		if !strings.Contains(dbName, SQLITE) {
			dsn = dbName + ".sqlite"
		} else {
			dsn = dbName
		}
		if options != "" {
			dsn += "?" + options
		} else {
			dsn += "?_pragma=foreign_keys(1)"
		}
	default:
		dbType = SQLITE
		klog.Printf("%s not handled, choices are: postgres,mysql,sqlite,maria,cockroach\n", dbType)
		dsn = dbName + ".sqlite"
		if dsn == "" {
			dsn = "db.sqlite"
		}
		if options != "" {
			dsn += "?" + options
		} else {
			dsn += "?_pragma=foreign_keys(1)"
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
		runned := InitShell()
		if runned {
			os.Exit(0)
		}
		onceDone = true
	}

	return nil
}

// SetCacheMaxMemory set max size of each cache cacheAllS AllM, minimum of 50 ...
func SetCacheMaxMemory(megaByte int) {
	if megaByte < 50 {
		megaByte = 50
	}
	cacheMaxMemoryMb = megaByte
	cacheAllM = kmap.New[dbCache, []map[string]any](false, cacheMaxMemoryMb)
	cacheAllS = kmap.New[dbCache, any](false, cacheMaxMemoryMb)
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
		klog.Printf("dialect can be sqlite, postgres, cockroach or mysql,maria only, not %s\n", dben.Dialect)
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
	if klog.CheckError(err) {
		return err
	}
	dbFound := false
	for _, dbb := range databases {
		if dbb.Conn == conn && dbb.Name == dbName {
			if dbb.Dialect == MARIA {
				dbb.Dialect = MYSQL
			} else if strings.HasPrefix(dbb.Dialect, "cockroach") {
				dbb.Dialect = POSTGRES
			}
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
			cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, _ ksbus.Channel) { handleCache(data) })
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
	cloneAndMigrateDashboard(staticAndTemplatesEmbeded...)
	initAdminUrlPatterns(serverBus.App, staticAndTemplatesEmbeded...)
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

// BeforeServersData handle connections and data received from another server
func BeforeServersData(fn func(data any, conn *ws.Conn)) {
	ksbus.BeforeServersData = fn
}

// BeforeDataWS handle connections and data received before upgrading websockets, useful to handle authentication
func BeforeDataWS(fn func(data map[string]any, conn *ws.Conn, originalRequest *http.Request) bool) {
	ksbus.BeforeDataWS = fn
}

// Transaction create new database/sql transaction and return it, it can be rollback ...
func Transaction(dbName ...string) (*sql.Tx, error) {
	return GetConnection(dbName...).Begin()
}

// FlushCache send msg to the cache system to Flush all the cache, safe to use in concurrent mode, and safe to use in general, it's done every 30 minutes(korm.FlushCacheEvery) and on update , create, delete , drop
func FlushCache() {
	go cachebus.Publish(CACHE_TOPIC, map[string]any{
		"type": "clean",
	})
}

// DisableCheck disable struct check for migrations on execution
func DisableCheck() {
	migrationAutoCheck = false
}

// DisableCache disable the cache system, if you are having problem with it, you can korm.FlushCache on command too
func DisableCache() {
	useCache = false
}

// GetConnection get connection of dbName, if not specified , it return default, first database connected
func GetConnection(dbName ...string) *sql.DB {
	var name string
	if len(dbName) > 0 {
		name = dbName[0]
	} else {
		name = databases[0].Name
	}
	db, err := GetMemoryDatabase(name)
	if klog.CheckError(err) {
		return nil
	}
	if db.Conn == nil {
		klog.Printf("rdmemory database %s not found", name)
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
		klog.Printf("rddatabase type not supported, should be sqlite, postgres, cockroach, maria or mysql")
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
	db, err := GetMemoryDatabase(dbName)
	if err != nil {
		return nil, err
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
			if v, ok := modelsPtrs[i].([]byte); ok {
				modelsPtrs[i] = string(v)
			}
			m[columns[i]] = modelsPtrs[i]
		}
		listMap = append(listMap, m)
	}
	if len(listMap) == 0 {
		return nil, errors.New("no data found")
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
