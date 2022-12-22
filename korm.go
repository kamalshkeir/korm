package korm

import (
	"database/sql"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
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
	// defaultDB keep tracking of the first database connected
	defaultDB         = ""
	useCache          = true
	databases         = []databaseEntity{}
	mModelTablename   = map[any]string{}
	cacheGetAllTables = kmap.New[string, []string](false)
	cachesOneM        = kmap.New[dbCache, map[string]any](false)
	cachesAllM        = kmap.New[dbCache, []map[string]any](false)
	relationsMap      = kmap.New[string, struct{}](false)

	onceDone       = false
	cachebus       *ksbus.Bus
	switchBusMutex sync.Mutex
	MaxOpenConns   = 10
	MaxIdleConns   = 10
	MaxLifetime    = 30 * time.Minute
	MaxIdleTime    = 12 * time.Hour
)

type Dialect = string

const (
	MIGRATION_FOLDER         = "migrations"
	CACHE_TOPIC              = "internal-db-cache"
	SQLITE           Dialect = "sqlite"
	POSTGRES         Dialect = "postgres"
	MYSQL            Dialect = "mysql"
	MARIA            Dialect = "maria"
	COCKROACH        Dialect = "cockroach"
)

type tableEntity struct {
	Types      map[string]string
	ModelTypes map[string]string
	Tags       map[string][]string
	Columns    []string
	Pk         string
	Name       string
}

type databaseEntity struct {
	Tables  []tableEntity
	Name    string
	Dialect string
	Conn    *sql.DB
}

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
	case MYSQL, MARIA, "mariadb":
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
		klog.Printf("%s not handled, choices are: postgres,mysql,sqlite,maria,coakroach\n", dbType)
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
		databases = append(databases, databaseEntity{
			Name:    dbName,
			Conn:    conn,
			Dialect: dbType,
			Tables:  []tableEntity{},
		})
	}
	if !onceDone {
		if useCache {
			cachebus = ksbus.New()
			cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, ch ksbus.Channel) {
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

func ManyToMany(table1, table2 string, dbName ...string) error {
	fkeys := []string{}
	autoinc := ""
	def := getMemoryDatabases()[0]
	mdbName := def.Name
	dbType := def.Dialect
	if len(dbName) > 0 {
		mdbName = dbName[0]
		dben, err := getMemoryDatabase(dbName[0])
		if err != nil {
			dbType = dben.Dialect
		}
	}

	defer func() {
		relationsMap.Set("m2m_"+table1+"-"+mdbName+"-"+table2, struct{}{})
	}()

	if _, ok := relationsMap.Get("m2m_" + table1 + "-" + mdbName + "-" + table2); ok {
		return nil
	}

	tables := GetAllTables(mdbName)
	if len(tables) == 0 {
		return errors.New("databse is empty")
	}
	for _, t := range tables {
		if t == table1+"_"+table2 || t == table2+"_"+table1 {
			return nil
		}
	}
	switch dbType {
	case SQLITE, "":
		autoinc = "INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"
	case POSTGRES, COCKROACH:
		autoinc = "SERIAL NOT NULL PRIMARY KEY"
	case MYSQL, MARIA:
		autoinc = "INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT"
	default:
		klog.Printf("dialect can be sqlite, postgres, coakroach or mysql,maria only, not %s\n", dbType)
	}
	//klog.Printfs("many to many field:%v\n", tableName+"_"+table)
	//klog.Printfs("table: %v, fields: %v, fkeys: %v, cols: %v, ftags:%v \n", tableName+"_"+table, res, fkeys, cols, mFieldName_Tags)

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
	err := Exec(mdbName, st)
	if err != nil {
		return err
	}

	return nil
}

// foreignkeyStat options are : "cascade","donothing", "noaction","setnull", "null","setdefault", "default"
func foreignkeyStat(fName, toTable, onDelete, onUpdate string) string {
	toPk := "id"
	if strings.Contains(toTable, ".") {
		sp := strings.Split(toTable, ".")
		if len(sp) == 2 {
			toPk = sp[1]
		}
	}
	fkey := "FOREIGN KEY (" + fName + ") REFERENCES " + toTable + "(" + toPk + ")"
	switch onDelete {
	case "cascade":
		fkey += " ON DELETE CASCADE"
	case "donothing", "noaction":
		fkey += " ON DELETE NO ACTION"
	case "setnull", "null":
		fkey += " ON DELETE SET NULL"
	case "setdefault", "default":
		fkey += " ON DELETE SET DEFAULT"
	}

	switch onUpdate {
	case "cascade":
		fkey += " ON UPDATE CASCADE"
	case "donothing", "noaction":
		fkey += " ON UPDATE NO ACTION"
	case "setnull", "null":
		fkey += " ON UPDATE SET NULL"
	case "setdefault", "default":
		fkey += " ON UPDATE SET DEFAULT"
	}
	return fkey
}

// NewSQLDatabaseFromConnection register a new database from connection, without the need for a dsn, if you are already connected to it
func NewFromConnection(dbType, dbName string, conn *sql.DB) error {
	if strings.HasPrefix(dbType, "cockroach") {
		dbType = POSTGRES
	}
	if dbType == MARIA || dbType == "mariadb" {
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
			if dbb.Dialect == "mariadb" || dbb.Dialect == MARIA {
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
		databases = append(databases, databaseEntity{
			Name:    dbName,
			Conn:    conn,
			Dialect: dbType,
			Tables:  []tableEntity{},
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
	switchBusMutex.Lock()
	cachebus = bus.Bus
	switchBusMutex.Unlock()
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
	migrationAutoCheck = false
}

// DisableCache disable the cache system, if you are having problem with it, you can korm.FlushCache on command too
func DisableCache() {
	useCache = false
}

func GetConnection(dbName ...string) (conn *sql.DB, ok bool) {
	var db *databaseEntity
	var err error
	if len(dbName) > 0 {
		db, err = getMemoryDatabase(dbName[0])
	} else {
		db, err = getMemoryDatabase(databases[0].Name)
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
	db, err := getMemoryDatabase(name)
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

	conn, ok := GetConnection(name)
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

	tb, err := getMemoryTable(table, dName)
	if err == nil {
		if len(tb.Types) > 0 {
			return tb.Types
		}
	}

	dbType := databases[0].Dialect
	conn, _ := GetConnection(dName)
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

// getMemoryTable get a table from memory for specified or first connected db
func getMemoryTable(tbName string, dbName ...string) (tableEntity, error) {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}
	db, err := getMemoryDatabase(dName)
	if err != nil {
		return tableEntity{}, err
	}
	for _, t := range db.Tables {
		if t.Name == tbName {
			return t, nil
		}
	}
	return tableEntity{}, errors.New("nothing found")
}

// getMemoryDatabases get all databases from memory
func getMemoryDatabases() []databaseEntity {
	return databases
}

// getMemoryDatabase return the first connected database korm.DefaultDatabase if dbName "" or "default" else the matched db
func getMemoryDatabase(dbName string) (*databaseEntity, error) {
	if defaultDB == "" {
		defaultDB = databases[0].Name
	}
	switch dbName {
	case "", "default":
		for i := range databases {
			if databases[i].Name == defaultDB {
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

func AddTrigger(onTable, col, bf_af_UpdateInsertDelete string, ofColumn, stmt string, forEachRow bool, whenEachRow string, dbName ...string) {
	stat := []string{}
	if len(dbName) == 0 {
		dbName = append(dbName, databases[0].Name)
	}
	if strings.Contains(strings.ToLower(ofColumn), "of") {
		ofColumn = strings.ReplaceAll(strings.ToLower(ofColumn), "of", "")
	}
	var dialect = ""
	db, err := getMemoryDatabase(dbName[0])
	if !klog.CheckError(err) {
		dialect = db.Dialect
	}
	switch dialect {
	case "sqlite", "sqlite3", "":
		if ofColumn != "" {
			ofColumn = " OF " + col
		}
		st := "CREATE TRIGGER IF NOT EXISTS " + onTable + "_trig_" + col + " "
		st += bf_af_UpdateInsertDelete + ofColumn + " ON " + onTable
		st += " BEGIN " + stmt + ";End;"
		stat = append(stat, st)
	case POSTGRES, "coakroach", "pg", "coakroachdb":
		if ofColumn != "" {
			ofColumn = " OF " + col
		}
		name := onTable + "_trig_" + col
		st := "CREATE OR REPLACE FUNCTION " + name + "_func() RETURNS trigger AS $$"
		st += " BEGIN " + stmt + ";RETURN NEW;"
		st += "END;$$ LANGUAGE plpgsql;"
		stat = append(stat, st)
		trigCreate := "CREATE OR REPLACE TRIGGER " + name
		trigCreate += " " + bf_af_UpdateInsertDelete + ofColumn + " ON public." + onTable
		trigCreate += " FOR EACH ROW EXECUTE PROCEDURE " + name + "_func();"
		stat = append(stat, trigCreate)
	case MYSQL, MARIA:
		stat = append(stat, "DROP TRIGGER "+onTable+"_trig_"+col+";")
		st := "CREATE TRIGGER " + onTable + "_trig_" + col + " "
		st += bf_af_UpdateInsertDelete + " ON " + onTable
		st += " FOR EACH ROW " + stmt + ";"
		stat = append(stat, st)
	default:
		return
	}

	if Debug {
		klog.Printf("statement: %s \n", stat)
	}

	for _, s := range stat {
		err := Exec(dbName[0], s)
		if err != nil {
			if !StringContains(err.Error(), "Trigger does not exist") {
				klog.Printf("rdcould not add trigger %v\n", err)
				return
			}
		}
	}
}

func DropTrigger(onField, tableName string, dbName ...string) {
	stat := "DROP TRIGGER " + tableName + "_trig_" + onField + ";"
	if Debug {
		klog.Printf("yl%s\n", stat)
	}
	n := databases[0].Name
	if len(dbName) > 0 {
		n = dbName[0]
	}
	err := Exec(n, stat)
	if err != nil {
		if !StringContains(err.Error(), "Trigger does not exist") {
			return
		}
		klog.Printf("rderr:%v\n", err)
	}
}
