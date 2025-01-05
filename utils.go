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
	"strings"
	"testing"
	"time"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/kstrct"
)

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
	case POSTGRES:
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
