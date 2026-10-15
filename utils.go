package korm

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/kstrct"
	"github.com/kamalshkeir/lg"
)

type JsonOption struct {
	As       string
	Dialect  string
	Database string
	Params   []any
}

func JSON_EXTRACT(dataJson string, opt ...JsonOption) string {
	var opts JsonOption
	if len(opt) > 0 {
		opts = opt[0]
	} else {
		return JSON_CAST(dataJson, "")
	}
	if opts.Dialect == "" {
		if opts.Database != "" {
			// db specified
			db, err := GetMemoryDatabase(opts.Database)
			if err != nil {
				db = &databases[0]
			}
			if opts.Dialect == "" {
				opts.Dialect = db.Dialect
			}
		} else {
			opts.Dialect = databases[0].Dialect
		}
	}

	isData := ""
	if strings.HasPrefix(dataJson, "'") {
		isData = "'"
	} else if strings.HasPrefix(dataJson, "{") {
		isData = "{"
	}

	switch opts.Dialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_EXTRACT("
		if isData == "{" {
			st += "'" + dataJson + "'"
		} else {
			st += dataJson
		}
		if len(opts.Params) == 0 {
			st += "),'$'"
			return st
		}
		st += ","

		for i, pp := range opts.Params {
			jsonParam := ""
			if v, ok := pp.(string); ok {
				jsonParam = v
			} else {
				jsonParam = fmt.Sprint(pp)
			}
			if i > 0 {
				st += ","
			}
			if strings.HasPrefix(jsonParam, "'") || strings.HasPrefix(jsonParam, "`") {
				jsonParam = jsonParam[1 : len(jsonParam)-2]
			}
			st += "'$"
			if strings.Contains(jsonParam, ".") {
				sp := strings.Split(jsonParam, ".")
				for _, s := range sp {
					// check if s is a number
					if _, err := strconv.Atoi(s); err == nil {
						st += "[" + s + "]"
					} else {
						st += "." + s
					}
				}
				st += "'"
			} else {
				st += "." + jsonParam + "'"
			}
			if i == len(opts.Params)-1 {
				st += ")"
			}
		}
		if opts.As != "" {
			st += " AS \"" + opts.As + "\""
		}
		return st
	case POSTGRES, "pg":
		if len(opts.Params) == 0 {
			return dataJson
		}
		if isData == "'" || isData == "{" {
			if isData == "{" {
				dataJson = "'" + dataJson + "'::jsonb"
			} else {
				dataJson = dataJson + "::jsonb"
			}
		}

		if len(opts.Params) == 1 {
			ss := ""
			if param, ok := opts.Params[0].(string); ok {
				for _, s := range strings.Split(param, ".") {
					ss += ", '" + s + "'"
				}
				return "JSONB_EXTRACT_PATH_TEXT(" + dataJson + ss + ")"
			} else {
				lg.ErrorC("param must be string", "param", param)
				return dataJson
			}
		}
		paramsString := ""
		// opts.ParamsToExtract "a.2.name", "b.email", "b"
		for i, extractParam := range opts.Params {
			ep := ""
			var ok bool
			if ep, ok = extractParam.(string); !ok {
				lg.ErrorC("param must be string", "param", extractParam)
				return dataJson
			}
			if i > 0 {
				paramsString += ", "
			}
			paramsString += "JSONB_EXTRACT_PATH_TEXT(" + dataJson
			for _, s := range strings.Split(ep, ".") {
				paramsString += ", '" + s + "'"
			}
			paramsString += ")"
		}
		if len(opts.Params) > 1 {
			paramsString = "JSONB_BUILD_ARRAY(" + paramsString + ")"
		}
		if opts.As != "" {
			paramsString += " AS \"" + opts.As + "\""
		}
		return paramsString
	default:
		lg.ErrorC("case not handled", "dialect", opts.Dialect)
		return ""
	}
}

func JSON_REMOVE(dataJson string, opt ...JsonOption) string {
	var opts JsonOption
	if len(opt) > 0 {
		opts = opt[0]
	} else {
		return JSON_CAST(dataJson, "")
	}
	if opts.Dialect == "" {
		if opts.Database != "" {
			// db specified
			db, err := GetMemoryDatabase(opts.Database)
			if err != nil {
				db = &databases[0]
			}
			if opts.Dialect == "" {
				opts.Dialect = db.Dialect
			}
		} else {
			opts.Dialect = databases[0].Dialect
		}
	}

	isData := ""
	if strings.HasPrefix(dataJson, "'") {
		isData = "'"
	} else if strings.HasPrefix(dataJson, "{") {
		isData = "{"
	}

	switch opts.Dialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_REMOVE("
		if isData == "{" {
			st += "'" + dataJson + "'"
		} else {
			st += dataJson
		}
		if len(opts.Params) == 0 {
			st += "),'$'"
			return st
		}
		st += ","

		for i, pp := range opts.Params {
			if i > 0 {
				st += ","
			}
			var jsonParam string
			var ok bool
			if jsonParam, ok = pp.(string); !ok {
				lg.ErrorC("expected string", "param", pp)
				return st
			}

			if i%2 == 0 {
				st += "'$"
				if strings.Contains(jsonParam, ".") {
					sp := strings.Split(jsonParam, ".")
					for _, s := range sp {
						// check if s is a number
						if _, err := strconv.Atoi(s); err == nil {
							st += "[" + s + "]"
						} else {
							st += "." + s
						}
					}
					st += "'"
				} else {
					jsonParam = strings.ReplaceAll(jsonParam, "'", "")
					st += "." + jsonParam + "'"
				}
				if i == len(opts.Params)-1 {
					st += ")"
				}
			} else {
				st += "'$"
				if _, err := strconv.Atoi(jsonParam); err == nil {
					st += "[" + jsonParam + "]"
				} else {
					st += "." + jsonParam + "'"
				}
				if i == len(opts.Params)-1 {
					st += ")"
				}
			}

		}
		if opts.As != "" {
			st += " AS \"" + opts.As + "\""
		}
		return st
	case POSTGRES, "pg":
		// your_column_name #- '{a, 2, name}' #- '{b, email}'
		if isData == "'" || isData == "{" {
			if isData == "{" {
				dataJson = "'" + dataJson + "'::jsonb"
			} else {
				dataJson = dataJson + "::jsonb"
			}
		}
		st := dataJson
		if len(opts.Params) == 0 {
			return st
		}
		for i, pp := range opts.Params {
			var jsonParam string
			var ok bool
			if jsonParam, ok = pp.(string); !ok && i%2 == 0 {
				lg.ErrorC("expected string", "param", jsonParam)
				return st
			}
			if i%2 == 0 && strings.Contains(jsonParam, ".") {
				sp := strings.Split(jsonParam, ".")
				tt := ""
				for i, s := range sp {
					if i > 0 {
						tt += ","
					}
					tt += s
				}
				st += " #- '{" + tt + "}'"
			} else {
				st += " - " + jsonParam
			}
		}
		if opts.As != "" {
			st += " AS \"" + opts.As + "\""
		}
		return st

	default:
		lg.ErrorC("case not handled", "dialect", opts.Dialect)
		return ""
	}
}

func JSON_SET(dataJson string, opt ...JsonOption) string {
	var opts JsonOption
	if len(opt) > 0 {
		opts = opt[0]
	} else {
		return JSON_CAST(dataJson, "data")
	}
	if len(opts.Params)%2 != 0 {
		lg.ErrorC("expected even number of params", "params", opts.Params)
		return dataJson
	}
	if opts.Dialect == "" {
		if opts.Database != "" {
			// db specified
			db, err := GetMemoryDatabase(opts.Database)
			if err != nil {
				db = &databases[0]
			}
			if opts.Dialect == "" {
				opts.Dialect = db.Dialect
			}
		} else {
			opts.Dialect = databases[0].Dialect
		}
	}

	isData := ""
	if strings.HasPrefix(dataJson, "'") {
		isData = "'"
	} else if strings.HasPrefix(dataJson, "{") {
		isData = "{"
	}

	switch opts.Dialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_SET("
		if isData == "{" {
			st += "'" + dataJson + "'"
		} else {
			st += dataJson
		}
		if len(opts.Params) == 0 {
			st += "),'$'"
			return st
		}
		st += ","

		for i, pp := range opts.Params {
			if i > 0 {
				st += ","
			}
			var jsonParam string
			var ok bool
			if i%2 == 0 {
				if jsonParam, ok = pp.(string); !ok {
					lg.ErrorC("expected string", "param", pp)
					return st
				}
				jsonParam = strings.ReplaceAll(jsonParam, "'", "")
				st += "'$"
				if strings.Contains(jsonParam, ".") {
					sp := strings.Split(jsonParam, ".")
					for _, s := range sp {
						// check if s is a number
						if _, err := strconv.Atoi(s); err == nil {
							st += "[" + s + "]"
						} else {
							st += "." + s
						}
					}
					st += "'"
				} else {
					st += "." + jsonParam + "'"
				}
			} else {
				if jp, ok := pp.(string); ok {
					if !strings.Contains(jp, "'") {
						jp = "'" + jp + "'"
					}
					st += jp
				} else {
					st += fmt.Sprint(pp)
				}
			}

			if i == len(opts.Params)-1 {
				st += ")"
			}
		}
		if opts.As != "" {
			st += " AS \"" + opts.As + "\""
		}
		return st
	case POSTGRES, "pg":
		if isData == "'" || isData == "{" {
			if isData == "{" {
				dataJson = "'" + dataJson + "'::jsonb"
			} else {
				dataJson = dataJson + "::jsonb"
			}
		}
		newws := make([]string, 0, len(opts.Params)/2)
		st := "JSONB_SET(" + dataJson
		if len(opts.Params) == 0 {
			return st + ")"
		} else if len(opts.Params) == 2 {
			for i, pp := range opts.Params {
				var jsonParam string
				var ok bool
				if i == 0 {
					if jsonParam, ok = pp.(string); !ok {
						lg.ErrorC("expected string", "param", pp)
						return dataJson
					}
				}
				st += ", "
				if i%2 == 0 && strings.Contains(jsonParam, ".") {
					tt := strings.ReplaceAll(jsonParam, ".", ",")
					tt = strings.ReplaceAll(tt, "'", "")
					st += " '{" + tt + "}'"
				} else {
					if v, ok := pp.(string); ok {
						if !strings.Contains(v, "'") {
							st += " '" + v + "'"
						} else {
							st += v
						}
					} else {
						st += fmt.Sprint(pp)
					}
				}
			}
			st += ", 'true'"
		} else {
			new := "JSONB_SET(" + dataJson
			for i, pp := range opts.Params {
				var jsonParam string
				var ok bool
				if i%2 == 0 {
					new = "JSONB_SET(%s"
					if jsonParam, ok = pp.(string); !ok {
						lg.ErrorC("expected string", "param", pp)
						return dataJson
					} else {
						pp = strings.ReplaceAll(jsonParam, "'", "")
					}
				}
				new += ", "
				if i%2 == 0 {
					tt := strings.ReplaceAll(jsonParam, ".", ",")
					new += " '{" + tt + "}'"
				} else {
					if v, ok := pp.(string); ok && i%2 == 1 {
						if !strings.Contains(v, "'") {
							new += " '" + v + "', 'true'"
						} else {
							new += " " + v + ", 'true'"
						}
					} else {
						new += fmt.Sprint(pp) + ", 'true'"
					}
					new += ")"
					newws = append(newws, new)
				}
			}
			res := newws[0]
			for i, v := range newws {
				if i == 0 {
					continue
				}
				res = fmt.Sprintf(res, v)
				if i == len(newws)-1 {
					res = fmt.Sprintf(res, dataJson)
				}
			}
			if opts.As != "" {
				res += " AS \"" + opts.As + "\""
			}
			return res
		}
		st += ")"
		if opts.As != "" {
			st += " AS \"" + opts.As + "\""
		}
		return st
	default:
		lg.ErrorC("case not handled", "dialect", opts.Dialect)
		return ""
	}
}

func JSON_ARRAY(values []any, as string, dialect ...string) string {
	dbDialect := databases[0].Dialect
	if len(dialect) > 0 {
		dbDialect = dialect[0]
	}
	valuesString := make([]string, 0, len(values))
	for _, v := range values {
		switch vType := v.(type) {
		case string:
			if strings.Contains(vType, "'") {
				valuesString = append(valuesString, vType)
			} else {
				valuesString = append(valuesString, "'"+vType+"'")
			}
		case time.Time:
			valuesString = append(valuesString, fmt.Sprint(vType.Unix()))
		case *time.Time:
			if vType != nil {
				valuesString = append(valuesString, fmt.Sprint(vType.Unix()))
			}
		default:
			valuesString = append(valuesString, fmt.Sprint(v))
		}
	}

	switch dbDialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_ARRAY(" + strings.Join(valuesString, ", ") + ")"
		if as != "" {
			st += " AS \"" + as + "\""
		}
		return st
	case POSTGRES, "pg":
		st := "JSONB_BUILD_ARRAY(" + strings.Join(valuesString, ", ") + ")"
		if as != "" {
			st += " AS \"" + as + "\""
		}
		return st
	default:
		lg.ErrorC("case not handled", "dialect", dbDialect)
		return ""
	}
}

func JSON_OBJECT(values []any, as string, dialect ...string) string {
	dbDialect := databases[0].Dialect
	if len(dialect) > 0 {
		dbDialect = dialect[0]
	}
	valuesString := make([]string, 0, len(values))
	for i, v := range values {
		var vv string
		if i%2 == 0 {
			var ok bool
			if vv, ok = v.(string); ok {
				if !strings.HasPrefix(vv, "'") {
					vv = "'" + vv + "'"
				}
			} else {
				lg.ErrorC("expected string", "value", v)
				return ""
			}
		} else {
			switch val := v.(type) {
			case string:
				if !strings.HasPrefix(val, "'") {
					vv = "'" + val + "'"
				} else {
					vv = val
				}
			case time.Time:
				vv = fmt.Sprint(val.Unix())
			case *time.Time:
				if val != nil {
					vv = fmt.Sprint(val.Unix())
				}
			default:
				vv = fmt.Sprint(v)
			}
		}

		valuesString = append(valuesString, vv)
	}

	switch dbDialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_OBJECT(" + strings.Join(valuesString, ", ") + ")"
		if as != "" {
			st += " AS \"" + as + "\""
		}
		return st
	case POSTGRES, "pg":
		st := "JSONB_BUILD_OBJECT(" + strings.Join(valuesString, ", ") + ")"
		if as != "" {
			st += " AS \"" + as + "\""
		}
		return st
	default:
		lg.ErrorC("case not handled", "dialect", dbDialect)
		return ""
	}
}

func JSON_CAST(value string, as string, dialect ...string) string {
	value = strings.ReplaceAll(value, "'", "\"")
	dbDialect := databases[0].Dialect
	if len(dialect) > 0 {
		dbDialect = dialect[0]
	}
	if !strings.HasPrefix(value, "'") {
		value = "'" + value + "'"
	}
	var st string
	switch dbDialect {
	case SQLITE, "sqlite":
		st = "JSON(" + value + ")"
	case POSTGRES, "pg":
		st = value + "::jsonb"
	case MYSQL:
		st = "JSON_EXTRACT(" + value + ", '$')"
	}
	if as != "" {
		st += " AS \"" + as + "\""
	}
	return st
}

func GenerateUUID() string {
	var uuid [16]byte
	_, err := io.ReadFull(rand.Reader, uuid[:])
	if err != nil {
		return ""
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant is 10
	var buf [36]byte
	encodeHex(buf[:], uuid)
	return string(buf[:])
}

func encodeHex(dst []byte, uuid [16]byte) {
	hex.Encode(dst, uuid[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], uuid[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], uuid[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], uuid[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], uuid[10:])
}

func RunEvery(t time.Duration, fn func(cancelChan chan struct{})) {
	//Usage : go RunEvery(2 * time.Second,func(){})
	cancel := make(chan struct{})
	fn(cancel)
	c := time.NewTicker(t)
loop:
	for {
		select {
		case <-c.C:
			fn(cancel)
		case <-cancel:
			break loop
		}
	}
}

func SliceContains[T comparable](elems []T, vs ...T) bool {
	for _, s := range elems {
		for _, v := range vs {
			if v == s {
				return true
			}
		}
	}
	return false
}

func DifferenceBetweenSlices[T comparable](slice1 []T, slice2 []T) []T {
	var diff []T

	// Loop two times, first to find slice1 strings not in slice2,
	// second loop to find slice2 strings not in slice1
	for i := 0; i < 2; i++ {
		for _, s1 := range slice1 {
			found := false
			for _, s2 := range slice2 {
				if s1 == s2 {
					found = true
					break
				}
			}
			// String not found. We add it to return slice
			if !found {
				diff = append(diff, s1)
			}
		}
		// Swap the slices, only if it was the first loop
		if i == 0 {
			slice1, slice2 = slice2, slice1
		}
	}

	return diff
}

func RemoveFromSlice[T comparable](slice *[]T, elemsToRemove ...T) {
	for i, elem := range *slice {
		for _, e := range elemsToRemove {
			if e == elem {
				*slice = append((*slice)[:i], (*slice)[i+1:]...)
			}
		}
	}
}

// Benchmark benchmark a function
func Benchmark(f func(), name string, iterations int) {
	// Start the timer
	start := time.Now()

	// Run the function multiple times
	var allocs int64
	for i := 0; i < iterations; i++ {
		allocs += int64(testing.AllocsPerRun(1, f))
	}

	// Stop the timer and calculate the elapsed time
	elapsed := time.Since(start)

	// Calculate the number of operations per second
	opsPerSec := float64(iterations) / elapsed.Seconds()

	// Calculate the number of allocations per operation
	allocsPerOp := float64(allocs) / float64(iterations)

	// Print the results
	fmt.Println("---------------------------")
	fmt.Println("Function", name)
	fmt.Printf("Operations per second: %f\n", opsPerSec)
	fmt.Printf("Allocations per operation: %f\n", allocsPerOp)
	fmt.Println("---------------------------")
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

// GetMemoryTable get a table from memory for specified or first connected db
func GetMemoryTableAndDB(tbName string, dbName ...string) (TableEntity, DatabaseEntity, error) {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}
	db, err := GetMemoryDatabase(dName)
	if err != nil {
		return TableEntity{}, DatabaseEntity{}, err
	}
	for _, t := range db.Tables {
		if t.Name == tbName {
			return t, *db, nil
		}
	}
	return TableEntity{}, DatabaseEntity{}, errors.New("nothing found")
}

// GetMemoryDatabases get all databases from memory
func GetMemoryDatabases() []DatabaseEntity {
	return databases
}

// GetMemoryDatabase return the first connected database korm.DefaultDatabase if dbName "" or "default" else the matched db
func GetMemoryDatabase(dbName string) (*DatabaseEntity, error) {
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

func DownloadFile(filepath string, url string) error {
	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

// getStructInfos very useful to access all struct fields data using reflect package
func getStructInfos[T any](strctt *T, ignoreZeroValues ...bool) (fields []string, fValues map[string]any, fTypes map[string]string, fTags map[string][]string) {
	fields = []string{}
	fValues = map[string]any{}
	fTypes = map[string]string{}
	fTags = map[string][]string{}

	s := reflect.ValueOf(strctt).Elem()
	typeOfT := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		fname := typeOfT.Field(i).Name
		fname = kstrct.ToSnakeCase(fname)
		fvalue := f.Interface()
		ftype := f.Type()

		if len(ignoreZeroValues) > 0 && ignoreZeroValues[0] && strings.Contains(ftype.Name(), "Time") {
			if v, ok := fvalue.(time.Time); ok {
				if v.IsZero() {
					continue
				}
			}
		}
		fields = append(fields, fname)
		if f.Type().Kind() == reflect.Ptr {
			fTypes[fname] = f.Type().Elem().String()
		} else {
			fTypes[fname] = f.Type().String()
		}
		fValues[fname] = fvalue
		if ftag, ok := typeOfT.Field(i).Tag.Lookup("korm"); ok {
			tags := strings.Split(ftag, ";")
			fTags[fname] = tags
		}
	}
	return fields, fValues, fTypes, fTags
}

func indexExists(conn *sql.DB, tableName, indexName string, dialect Dialect) bool {
	var query string
	switch dialect {
	case SQLITE:
		query = `SELECT name FROM sqlite_master 
				 WHERE type='index' AND tbl_name=? AND name=?`
	case POSTGRES, COCKROACH:
		query = `SELECT indexname FROM pg_indexes 
				 WHERE tablename = $1 AND indexname = $2`
	case MYSQL, MARIA:
		query = `SELECT INDEX_NAME FROM information_schema.statistics 
				 WHERE table_name = ? AND index_name = ?`
	default:
		return false
	}

	var name string
	err := conn.QueryRow(query, tableName, indexName).Scan(&name)
	return err == nil
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

func IsValidEmail(email string) bool {
	_, err := mail.ParseAddress(email)
	return err == nil
}

// SetCacheMaxMemory set max size of each cache cacheAllS AllM, minimum of 50 ...
func SetCacheMaxMemory(megaByte int) {
	if megaByte < 100 {
		megaByte = 100
	}
	cacheMaxMemoryMb = megaByte
	caches = kmap.New[string, *kmap.SafeMap[dbCache, any]](cacheMaxMemoryMb)
}

// SystemMetrics holds memory and runtime statistics for the application
type SystemMetrics struct {
	// Memory metrics
	HeapMemoryMB   float64 // Currently allocated heap memory in MB
	SystemMemoryMB float64 // Total memory obtained from system in MB
	StackMemoryMB  float64 // Memory used by goroutine stacks
	HeapObjects    uint64  // Number of allocated heap objects
	HeapReleasedMB float64 // Memory released to the OS in MB

	// Garbage Collection metrics
	NumGC         uint32  // Number of completed GC cycles
	LastGCTimeSec float64 // Time since last garbage collection in seconds
	GCCPUPercent  float64 // Fraction of CPU time used by GC (0-100)

	// Runtime metrics
	NumGoroutines int    // Current number of goroutines
	NumCPU        int    // Number of logical CPUs
	GoVersion     string // Go version used to build the program
}

// GetSystemMetrics returns memory and runtime statistics for the application
func GetSystemMetrics() SystemMetrics {
	var metrics SystemMetrics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Memory metrics
	metrics.HeapMemoryMB = float64(m.Alloc) / (1024 * 1024)
	metrics.SystemMemoryMB = float64(m.Sys) / (1024 * 1024)
	metrics.StackMemoryMB = float64(m.StackSys) / (1024 * 1024)
	metrics.HeapObjects = m.HeapObjects
	metrics.HeapReleasedMB = float64(m.HeapReleased) / (1024 * 1024)

	// GC metrics
	metrics.NumGC = m.NumGC
	metrics.LastGCTimeSec = time.Since(time.Unix(0, int64(m.LastGC))).Seconds()
	metrics.GCCPUPercent = m.GCCPUFraction * 100

	// Runtime metrics
	metrics.NumGoroutines = runtime.NumGoroutine()
	metrics.NumCPU = runtime.NumCPU()
	metrics.GoVersion = runtime.Version()

	return metrics
}

// PrintSystemMetrics prints the current system metrics
func PrintSystemMetrics() {
	metrics := GetSystemMetrics()
	fmt.Println("Memory Metrics:")
	fmt.Printf("  Heap Memory: %.2f MB\n", metrics.HeapMemoryMB)
	fmt.Printf("  System Memory: %.2f MB\n", metrics.SystemMemoryMB)
	fmt.Printf("  Stack Memory: %.2f MB\n", metrics.StackMemoryMB)
	fmt.Printf("  Heap Objects: %d\n", metrics.HeapObjects)
	fmt.Printf("  Heap Released: %.2f MB\n", metrics.HeapReleasedMB)

	fmt.Println("\nGarbage Collection:")
	fmt.Printf("  GC Cycles: %d\n", metrics.NumGC)
	fmt.Printf("  Last GC: %.2f seconds ago\n", metrics.LastGCTimeSec)
	fmt.Printf("  GC CPU Usage: %.2f%%\n", metrics.GCCPUPercent)

	fmt.Println("\nRuntime Info:")
	fmt.Printf("  Goroutines: %d\n", metrics.NumGoroutines)
	fmt.Printf("  CPUs: %d\n", metrics.NumCPU)
	fmt.Printf("  Go Version: %s\n", metrics.GoVersion)
}
