package korm

import (
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/kamalshkeir/kinput"
	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kstrct"
)

var checkEnabled = true

// DisableCheck disable struct changes check
func DisableCheck() {
	checkEnabled = false
}

type kormFkey struct {
	FromTableField string
	ToTableField   string
	Unique         bool
}

// LinkModel link a struct model to a  db_table_name
func LinkModel[T any](to_table_name string, dbName ...string) {
	var db *DatabaseEntity
	if len(dbName) == 0 && len(databases) > 0 {
		db = &databases[0]
	} else {
		dbb, err := GetMemoryDatabase(dbName[0])
		if klog.CheckError(err) {
			return
		}
		db = dbb
	}

	fields, _, ftypes, ftags := getStructInfos(new(T))
	// get columns from db
	colsNameType, _ := GetAllColumnsTypes(to_table_name, db.Name)

	pk := ""
tagsLoop:
	for col, tags := range ftags {
		for i := range tags {
			tags[i] = strings.TrimSpace(tags[i])
			if tags[i] == "pk" || tags[i] == "autoinc" {
				pk = col
				break tagsLoop
			}
		}
	}

	tFound := false
	for _, t := range db.Tables {
		if t.Name == to_table_name {
			tFound = true
		}
	}
	var kfkeys = []kormFkey{}
	if !tFound {
	loop:
		for k, v := range ftypes {
			if v, ok := ftags[k]; ok {
				for _, t := range v {
					if t == "-" || t == "skip" {
						for i := range fields {
							if fields[i] == k {
								fields = append(fields[:i], fields[i+1:]...)
							}
						}
						delete(ftags, k)
						delete(ftypes, k)
						delete(colsNameType, k)
						continue loop
					} else if strings.HasPrefix(t, "fk:") {
						st := strings.Split(t, ":")[1]
						fkey := kormFkey{
							FromTableField: to_table_name + "." + kstrct.ToSnakeCase(k),
							ToTableField:   st,
						}
						if strings.Contains(strings.Join(ftags[k], ";"), "unique") {
							fkey.Unique = true
						}
						kfkeys = append(kfkeys, fkey)
					}
				}
			}

			if v == "" {
				for i := len(fields) - 1; i >= 0; i-- {
					if fields[i] == k {
						fields = append(fields[:i], fields[i+1:]...)
					}
				}
				delete(ftags, k)
				delete(ftypes, k)
				delete(colsNameType, k)
			} else if (unicode.IsUpper(rune(v[0])) || strings.Contains(v, ".")) && !strings.HasSuffix(v, "Time") {
				// if struct
				for i := len(fields) - 1; i >= 0; i-- {
					if fields[i] == k {
						fields = append(fields[:i], fields[i+1:]...)
					}
				}
				delete(ftags, k)
				delete(ftypes, k)
				delete(colsNameType, k)
			}
		}
		te := TableEntity{
			Fkeys:      kfkeys,
			Name:       to_table_name,
			Columns:    fields,
			ModelTypes: ftypes,
			Types:      colsNameType,
			Tags:       ftags,
			Pk:         pk,
		}
		db.Tables = append(db.Tables, te)
		// sync model
		if checkEnabled {
			if len(te.Types) > len(te.Columns) {
				removedCols := []string{}
				colss := []string{}
				for dbcol := range te.Types {
					found := false
					for _, fname := range te.Columns {
						if fname == dbcol {
							found = true
						}
					}
					if !found {
						// remove dbcol from db
						klog.Printfs("rd⚠️ field '%s' has been removed from '%T'\n", dbcol, *new(T))
						removedCols = append(removedCols, dbcol)
					} else {
						colss = append(colss, dbcol)
					}
				}
				if len(removedCols) > 0 {
					choice, err := kinput.String(kinput.Yellow, "> do we remove extra columns ? (Y/n): ")
					klog.CheckError(err)
					switch choice {
					case "y", "Y":
						temp := te.Name + "_temp"
						tempQuery, err := autoMigrate(new(T), db, temp, true)
						if klog.CheckError(err) {
							return
						}
						if Debug {
							fmt.Println("DEBUG:SYNC:", tempQuery)
						}
						cls := strings.Join(colss, ",")
						_, err = db.Conn.Exec("INSERT INTO " + temp + " (" + cls + ") SELECT " + cls + " FROM " + te.Name)
						if klog.CheckError(err) {
							return
						}
						_, err = Table(te.Name + "_old").Database(db.Name).Drop()
						if klog.CheckError(err) {
							return
						}
						_, err = db.Conn.Exec("ALTER TABLE " + te.Name + " RENAME TO " + te.Name + "_old")
						if klog.CheckError(err) {
							return
						}
						_, err = db.Conn.Exec("ALTER TABLE " + temp + " RENAME TO " + te.Name)
						if klog.CheckError(err) {
							return
						}
						klog.Printfs("grDone, you can still find your old table with the same data %s\n", te.Name+"_old")
						os.Exit(0)
					default:
						return
					}
				}
			} else if len(te.Types) < len(te.Columns) {
				addedFields := []string{}
				for _, fname := range te.Columns {
					if _, ok := te.Types[fname]; !ok {
						klog.Printfs("rd⚠️ column '%s' is missing from table '%s'\n", fname, to_table_name)
						addedFields = append(addedFields, fname)
					}
				}
				if len(addedFields) > 0 {
					choice, err := kinput.String(kinput.Yellow, "> do we add missing columns ? (Y/n): ")
					klog.CheckError(err)
					switch choice {
					case "y", "Y":
						temp := te.Name + "_temp"
						tempQuery, err := autoMigrate(new(T), db, temp, true)
						if klog.CheckError(err) {
							return
						}
						if Debug {
							fmt.Println("DEBUG:SYNC:", tempQuery)
						}
						var colss []string

						for k := range te.Types {
							colss = append(colss, k)
						}
						cls := strings.Join(colss, ",")
						_, err = db.Conn.Exec("INSERT INTO " + temp + " (" + cls + ") SELECT " + cls + " FROM " + te.Name)
						if klog.CheckError(err) {
							klog.Printfs("query: %s\n", "INSERT INTO "+temp+" ("+cls+") SELECT "+cls+" FROM "+te.Name)
							return
						}
						_, err = Table(te.Name + "_old").Database(db.Name).Drop()
						if klog.CheckError(err) {
							return
						}
						_, err = db.Conn.Exec("ALTER TABLE " + te.Name + " RENAME TO " + te.Name + "_old")
						if klog.CheckError(err) {
							return
						}
						_, err = db.Conn.Exec("ALTER TABLE " + temp + " RENAME TO " + te.Name)
						if klog.CheckError(err) {
							return
						}
						klog.Printfs("grDone, you can still find your old table with the same data %s\n", te.Name+"_old")
						os.Exit(0)
					default:
						return
					}
				}
			}
		}

	}
}

func flushCache() {
	go func() {
		cacheAllM.Flush()
		cacheAllS.Flush()
		cacheQueryS.Flush()
		cacheQueryM.Flush()
		cacheQ.Flush()
		cachesOneM.Flush()
		cacheOneS.Flush()
		cacheAllTables.Flush()
		cacheAllCols.Flush()
	}()
}
