package korm

import (
	"strings"

	"github.com/kamalshkeir/klog"
)

// linkModel link a struct model to a  db_table_name
func linkModel[T any](to_table_name string, db *DatabaseEntity) {
	if db.Name == "" {
		var err error
		db.Name = databases[0].Name
		db, err = GetMemoryDatabase(db.Name)
		if klog.CheckError(err) {
			return
		}
	}
	fields, _, ftypes, ftags := getStructInfos(new(T))
	// get columns from db
	colsNameType := GetAllColumnsTypes(to_table_name, db.Name)

	pk := ""
	for col, tags := range ftags {
		for i := range tags {
			tags[i] = strings.TrimSpace(tags[i])
			if tags[i] == "pk" || tags[i] == "autoinc" {
				pk = col
				break
			}
		}
	}

	tFound := false
	for _, t := range db.Tables {
		if t.Name == to_table_name {
			tFound = true
		}
	}

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
		cachesOneM.Flush()
		cacheOneS.Flush()
		cacheAllTables.Flush()
		cacheAllCols.Flush()
	}()
}
