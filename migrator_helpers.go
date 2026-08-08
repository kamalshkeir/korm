package korm

import (
	"strings"
	"unicode"

	"github.com/kamalshkeir/kstrct"
	"github.com/kamalshkeir/lg"
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
		if lg.CheckError(err) {
			return
		}
		db = dbb
	}

	tFound := false
	for _, t := range db.Tables {
		if t.Name == to_table_name {
			tFound = true
		}
	}
	var kfkeys = []kormFkey{}
	// get columns from db
	colsNameType, _ := GetAllColumnsTypes(to_table_name, db.Name)
	fields, _, ftypes, ftags := getStructInfos(new(T))
	pk := ""
	if !tFound {
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
			} else if (unicode.IsUpper(rune(v[0])) || strings.Contains(v, ".")) && !strings.HasSuffix(v, "Time") && !(strings.HasPrefix(v, "map") || strings.HasPrefix(v, "*map")) {
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
		if pk == "" {
			if strings.HasSuffix(fields[0], "id") {
				pk = fields[0]
			} else {
				pk = "id"
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

		if to_table_name != "_tables_infos" {
			// insert tables infos into db
			mTablesInfos, err := Model[TablesInfos]().Where("name = ?", to_table_name).One()
			if err != nil {
				// adapt table_infos and insert
				fktbinfos := []string{}
				for _, fk := range te.Fkeys {
					un := "false"
					if fk.Unique {
						un = "true"
					}
					st := fk.FromTableField + ";;" + fk.ToTableField + ";;" + un
					fktbinfos = append(fktbinfos, st)
				}
				types := make([]string, 0, len(te.Types))
				for k, v := range te.Types {
					types = append(types, k+":"+v)
				}
				model_types_in := make([]string, 0, len(te.ModelTypes))
				for k, v := range te.ModelTypes {
					model_types_in = append(model_types_in, k+":"+v)
				}
				tags_in := make([]string, 0, len(te.Tags))
				for k, v := range te.Tags {
					tags_in = append(tags_in, k+":"+strings.Join(v, ","))
				}
				_, err = Table("_tables_infos").Insert(map[string]any{
					"pk":          pk,
					"name":        to_table_name,
					"columns":     strings.Join(fields, ","),
					"fkeys":       strings.Join(fktbinfos, ","),
					"types":       strings.Join(types, ";;"),
					"model_types": strings.Join(model_types_in, ";;"),
					"tags":        strings.Join(tags_in, ";;"),
				})
				lg.CheckError(err)
			} else {
				fkk := []kormFkey{}
				for _, fk := range mTablesInfos.Fkeys {
					sp := strings.Split(fk, ";;")
					if len(sp) == 3 {
						k := kormFkey{}
						for i, spp := range sp {
							spp = strings.TrimSpace(spp)
							switch i {
							case 0:
								k.FromTableField = spp
							case 1:
								k.ToTableField = spp
							case 2:
								if spp == "true" {
									k.Unique = true
								}
							}
						}
						fkk = append(fkk, k)
					}
				}
				tee := TableEntity{
					Types:      mTablesInfos.Types,
					ModelTypes: mTablesInfos.ModelTypes,
					Tags:       mTablesInfos.Tags,
					Columns:    mTablesInfos.Columns,
					Pk:         mTablesInfos.Pk,
					Name:       mTablesInfos.Name,
					Fkeys:      fkk,
				}
				if !kstrct.CompareStructs(te, tee) {
					// update
					// adapt table_infos and insert
					fktbinfos := []string{}
					for _, fk := range te.Fkeys {
						un := "false"
						if fk.Unique {
							un = "true"
						}
						st := fk.FromTableField + ";;" + fk.ToTableField + ";;" + un
						fktbinfos = append(fktbinfos, st)
					}
					types := make([]string, 0, len(te.Types))
					for k, v := range te.Types {
						types = append(types, k+":"+v)
					}
					model_types_in := make([]string, 0, len(te.ModelTypes))
					for k, v := range te.ModelTypes {
						model_types_in = append(model_types_in, k+":"+v)
					}
					tags_in := make([]string, 0, len(te.Tags))
					for k, v := range te.Tags {
						tags_in = append(tags_in, k+":"+strings.Join(v, ","))
					}
					_, err = Table("_tables_infos").Where("name = ?", to_table_name).SetM(map[string]any{
						"pk":          pk,
						"name":        to_table_name,
						"columns":     strings.Join(fields, ","),
						"fkeys":       strings.Join(fktbinfos, ","),
						"types":       strings.Join(types, ";;"),
						"model_types": strings.Join(model_types_in, ";;"),
						"tags":        strings.Join(tags_in, ";;"),
					})
					lg.CheckError(err)
				}
			}
		}
	}
}

func flushCache() {
	caches.Flush()
	cacheQ.Flush()
	cacheAllTables.Flush()
	cacheAllCols.Flush()
}
