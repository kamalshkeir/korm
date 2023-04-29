package korm

import (
	"strings"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kstrct"
)

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
	colsNameType := GetAllColumnsTypes(to_table_name, db.Name)

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
			if strings.HasPrefix(v, "[]") {
				for i := range fields {
					if fields[i] == k {
						fields = append(fields[:i], fields[i+1:]...)
					}
				}
				delete(ftags, k)
				delete(ftypes, k)
				delete(colsNameType, k)
			} else if strings.Contains(v, ".") && !strings.HasSuffix(v, "Time") {
				// if struct
				for i := range fields {
					if fields[i] == k {
						fields = append(fields[:i], fields[i+1:]...)
					}
				}
				delete(ftags, k)
				delete(ftypes, k)
				delete(colsNameType, k)
			}
		}
		db.Tables = append(db.Tables, TableEntity{
			Fkeys:      kfkeys,
			Name:       to_table_name,
			Columns:    fields,
			ModelTypes: ftypes,
			Types:      colsNameType,
			Tags:       ftags,
			Pk:         pk,
		})
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
