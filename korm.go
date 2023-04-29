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
	"reflect"
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
	mModelTablename     = map[string]any{}
	cacheAllTables      = kmap.New[string, []string](false)
	cacheAllCols        = kmap.New[string, map[string]string](false)
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
	cstm := dbType
	if useCache {
		sql.Register("korm", Wrap(dbDriver, &myLogAndCacheHook{}))
		cstm = "korm"
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
func WithDocs(generateJsonDocs bool, outJsonDocs string, handlerMiddlewares ...func(handler kmux.Handler) kmux.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus()
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
	serverBus.App.Get(webPath+"/*path", handler)
	return serverBus
}

// WithEmbededDocs same as WithDocs but embeded, enable swagger docs at DocsUrl default to '/docs/'
func WithEmbededDocs(embeded embed.FS, embededDirPath string, handlerMiddlewares ...func(handler kmux.Handler) kmux.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus()
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
		statement = "select column_name,data_type FROM information_schema.columns WHERE table_name = '" + table + "'"
	case MYSQL, MARIA:
		statement = "select column_name,data_type FROM information_schema.columns WHERE table_name = '" + table + "' AND TABLE_SCHEMA = '" + db.Name + "'"
	default:
		statement = "pragma table_info(" + table + ");"
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

type Build[T any] struct {
	dest    T
	rows    *sql.Rows
	ref     reflect.Value
	stat    string
	typ     string
	dialect string
	err     error
	cols    []string
	res     []T
}

func Q[T any](statement string, args ...any) *Build[T] {
	var db *DatabaseEntity
	if len(databases) == 1 {
		db = &databases[0]
	} else if len(args) > 0 {
		if v, ok := args[len(args)-1].(string); ok {
			if strings.HasPrefix(v, "db:") {
				db, _ = GetMemoryDatabase(strings.TrimSpace(v[3:]))
			}
		}
		if db == nil {
			db = &databases[0]
		}
	} else if len(databases) > 1 {
		db = &databases[0]
	} else {
		builder := &Build[T]{
			err: fmt.Errorf("db not found"),
		}
		return builder
	}
	if useCache {
		stt := db.Name + statement + fmt.Sprint(args...)
		if v, ok := cacheQ.Get(stt); ok {
			if vv, ok := v.([]T); ok {
				return &Build[T]{
					res:  vv,
					stat: stt,
				}
			}
		}
	}

	builder := &Build[T]{
		dialect: db.Dialect,
		dest:    *new(T),
		stat:    db.Name + statement + fmt.Sprint(args...),
	}
	builder.typ = fmt.Sprintf("%T", builder.dest)
	builder.ref = reflect.ValueOf(builder.dest)
	if builder.typ[1] == '[' {
		builder.err = fmt.Errorf("type param cannot be slice")
		return builder
	}
	if db.Conn == nil {
		builder.err = fmt.Errorf("no connection")
		return builder
	}
	adaptPlaceholdersToDialect(&statement, db.Dialect)
	adaptTimeToUnixArgs(&args)
	rows, err := db.Conn.Query(statement, args...)
	if err != nil {
		builder.err = err
		return builder
	}
	columns, err := rows.Columns()
	if err != nil {
		builder.err = err
		return builder
	}
	builder.rows = rows
	builder.cols = columns
	return builder
}

func (nb *Build[T]) Error() error {
	return nb.err
}

func (nb *Build[T]) To(dest *[]T, nested ...bool) *Build[T] {
	if useCache && len(nb.res) > 0 {
		if len(nb.typ) >= 4 && nb.typ[:4] == "chan" {
			for _, r := range nb.res {
				kk := (*dest)[0]
				if dd, ok := any(kk).(chan T); ok {
					dd <- r
				}
			}
			return nb
		} else {
			*dest = nb.res
			return nb
		}
	}
	if nb.err != nil || nb.rows == nil {
		return nb
	}
	isMap, isChan, isStrct, isArith, isPtr := false, false, false, false, false
	isNested := len(nested) > 0 && nested[0]
	if nb.typ[0] == '*' {
		isPtr = true
		nb.typ = nb.typ[1:]
	}
	if len(nb.typ) >= 4 && nb.typ[:4] == "chan" {
		isChan = true
		if strings.Contains(nb.typ, "map") {
			isMap = true
		}
	}
	if nb.ref.Kind() == reflect.Struct {
		isStrct = true
	} else if nb.typ[:3] == "map" {
		isMap = true
	} else {
		isArith = true
	}
	var (
		columns_ptr_to_values = make([]any, len(nb.cols))
		temp                  = new(T)
		lastData              []kstrct.KV
		values                = make([]any, len(nb.cols))
	)
	if isNested {
		lastData = make([]kstrct.KV, 0, len(nb.cols))
	}
	index := 0
	kv := make([]kstrct.KV, 0, len(nb.cols))
	defer nb.rows.Close()
loop:
	for nb.rows.Next() {
		kv = kv[:]
		for i := range values {
			columns_ptr_to_values[i] = &values[i]
		}
		err := nb.rows.Scan(columns_ptr_to_values...)
		if err != nil {
			nb.err = errors.Join(err, nb.err)
			return nb
		}
		for i, key := range nb.cols {
			if nb.dialect == MYSQL {
				if v, ok := values[i].([]byte); ok {
					values[i] = string(v)
				}
			}
			kv = append(kv, kstrct.KV{Key: key, Value: values[i]})
		}
		switch {
		case isStrct && !isChan:
			if isPtr {
				nb.err = errors.Join(nb.err, fmt.Errorf("slice of pointer structs are not allowed"))
				return nb
			} else {
				if !isNested {
					err := kstrct.FillFromKV(temp, kv)
					if klog.CheckError(err) {
						nb.err = errors.Join(err, nb.err)
						return nb
					}
					*dest = append(*dest, *temp)
					continue loop
				}

				if len(lastData) == 0 {
					lastData = kv
					*dest = append(*dest, *new(T))
					temp = &(*dest)[0]
				}
				for _, kvv := range kv {
					if kvv.Key == nb.cols[0] {
						foundk := false
						for _, ld := range lastData {
							if ld.Key == nb.cols[0] && ld.Value == kvv.Value {
								foundk = true
								lastData = kv
							}
						}
						if !foundk {
							lastData = kv
							index++
							*dest = append(*dest, *new(T))
							temp = &(*dest)[index]
						}
					}
				}
				err := kstrct.FillFromKV(temp, kv)
				if klog.CheckError(err) {
					nb.err = errors.Join(err, nb.err)
					return nb
				}
			}
			continue loop
		case isMap && !isChan:
			if isPtr {
				nb.err = errors.Join(nb.err, fmt.Errorf("slice of pointer to map are not allowed, use []map instead"))
				return nb
			} else {
				m := make(map[string]any, len(kv))
				for _, kvv := range kv {
					m[kvv.Key] = kvv.Value
				}
				if v, ok := any(m).(T); ok {
					*dest = append(*dest, v)
				}
			}
			continue loop
		case isArith && !isChan:
			if len(kv) == 1 {
				for _, vKV := range kv {
					vv := vKV.Value
					if isPtr {
						if vok, ok := any(&vv).(T); ok {
							*dest = append(*dest, vok)
						} else {
							elem := reflect.New(nb.ref.Type()).Elem()
							err := kstrct.SetReflectFieldValue(elem, vKV.Value)
							if err != nil {
								nb.err = errors.Join(nb.err, err)
								return nb
							}
							*dest = append(*dest, elem.Interface().(T))
						}
					} else {
						if vok, ok := any(vv).(T); ok {
							*dest = append(*dest, vok)
						} else {
							elem := reflect.New(nb.ref.Type()).Elem()
							err := kstrct.SetReflectFieldValue(elem, vKV.Value)
							if err != nil {
								nb.err = errors.Join(nb.err, err)
								return nb
							}
							inter := elem.Interface()
							*dest = append(*dest, inter.(T))
						}
					}
				}
				continue loop
			}
		case isChan:
			switch {
			case isStrct:
				if !isNested {
					err := kstrct.FillFromKV((*dest)[0], kv)
					if klog.CheckError(err) {
						nb.err = errors.Join(err, nb.err)
						return nb
					}
					continue loop
				}
				update := false
				if len(lastData) == 0 {
					update = true
				}
				for _, kvv := range kv {
					if kvv.Key == nb.cols[0] {
						foundk := false
						for _, ld := range lastData {
							if ld.Key == nb.cols[0] && ld.Value == kvv.Value {
								foundk = true
							}
						}
						if !foundk {
							update = true
							temp = new(T)
						}
					}
				}
				lastData = kv
				if update {
					chanType := reflect.New(nb.ref.Type().Elem()).Elem()
					for _, vKv := range kv {
						err := kstrct.SetReflectFieldValue(chanType, vKv.Value)
						if klog.CheckError(err) {
							nb.err = errors.Join(nb.err, err)
							return nb
						}
						reflect.ValueOf((*dest)[0]).Send(chanType)
					}
					continue loop
				}
			case isMap:
				m := make(map[string]any, len(kv))
				for _, vkv := range kv {
					m[vkv.Key] = vkv.Value
				}
				if v, ok := any((*dest)[0]).(chan map[string]any); ok {
					v <- m
				} else {
					nb.err = errors.Join(nb.err, fmt.Errorf("expected *[]chan map[string]any"))
					return nb
				}
				continue loop
			case isArith:

				chanType := reflect.New(nb.ref.Type().Elem()).Elem()
				for _, vKv := range kv {
					if chanType.Kind() == reflect.Struct || chanType.Elem().Kind() == reflect.Struct {
						m := make(map[string]any, len(kv))
						for _, vkv := range kv {
							m[vkv.Key] = vkv.Value
						}
						err := kstrct.SetReflectFieldValue(chanType, m)
						if klog.CheckError(err) {
							nb.err = errors.Join(nb.err, err)
							return nb
						}
					} else {
						err := kstrct.SetReflectFieldValue(chanType, vKv.Value)
						if klog.CheckError(err) {
							nb.err = errors.Join(nb.err, err)
							return nb
						}
					}
					reflect.ValueOf((*dest)[0]).Send(chanType)
				}
			default:
				nb.err = errors.Join(nb.err, fmt.Errorf("channel case not handled"))
				return nb
			}
		default:
			nb.err = errors.Join(nb.err, fmt.Errorf("default triggered, case not handled"))
			return nb
		}
	}
	if useCache && !isChan && len(*dest) > 0 {
		cacheQ.Set(nb.stat, *dest)
	}
	return nb
}

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
