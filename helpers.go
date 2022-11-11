package korm

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kamalshkeir/kinput"
	"github.com/kamalshkeir/klog"
)

type dbCache struct {
	limit      int
	page       int
	database   string
	table      string
	selected   string
	orderBys   string
	whereQuery string
	query      string
	offset     string
	statement  string
	args       string
}

// LinkModel link a struct model to a  db_table_name
func LinkModel[T comparable](to_table_name string, db *DatabaseEntity) {
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
	cols := []string{}
	for k := range colsNameType {
		cols = append(cols, k)
	}
	for _, list := range ftags {
		for i := range list {
			list[i] = strings.TrimSpace(list[i])
		}
	}
	pk := ""
	for col, tags := range ftags {
		if SliceContains(tags, "autoinc", "pk") {
			pk = col
			break
		}
	}

	diff := Difference(fields, cols)
	if pk == "" {
		pk = "id"
		ftypes["id"] = "int"
		if !SliceContains(fields, "id") {
			fields = append([]string{"id"}, fields...)
		}
		SliceRemove(&diff, "id")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// add or remove field from struct
		handleAddOrRemove[T](to_table_name, fields, cols, diff, db, ftypes, ftags, pk)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		// rename field
		handleRename(to_table_name, fields, cols, diff, db, ftags, pk)
	}()
	wg.Wait()

	tFound := false
	for _, t := range db.Tables {
		if t.Name == to_table_name {
			tFound = true
		}
	}

	if !tFound {
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

// handleAddOrRemove handle sync with db when adding or removing from a struct auto migrated
func handleAddOrRemove[T comparable](to_table_name string, fields, cols, diff []string, db *DatabaseEntity, ftypes map[string]string, ftags map[string][]string, pk string) {
	if len(cols) > len(fields) { // extra column db
		for _, d := range diff {
			fileName := "drop_" + to_table_name + "_" + d + ".sql"
			if v, ok := ftags[d]; ok && v[0] == "-" || d == pk {
				continue
			}
			if _, err := os.Stat("migrations/" + fileName); err == nil {
				continue
			}
			fmt.Println(" ")
			klog.Printfs("⚠️ found extra column '%s' in the database table '%s'\n", d, to_table_name)

			statement := "ALTER TABLE " + to_table_name + " DROP COLUMN " + d

			choice := kinput.Input(kinput.Yellow, "> do you want to remove '"+d+"' from database ?, you can also generate the query using 'g' (Y/g/n): ")
			if SliceContains([]string{"yes", "Y", "y"}, choice) {
				sst := "DROP INDEX IF EXISTS idx_" + to_table_name + "_" + d
				trigs := "DROP TRIGGER IF EXISTS " + to_table_name + "_update_trig"
				if len(databases) > 1 && db.Name == "" {
					ddb := kinput.Input(kinput.Blue, "> There are more than one database connected, enter database name: ")
					conn := GetSQLConnection(ddb)
					if conn != nil {
						// triggers
						if db.Dialect != MYSQL && db.Dialect != MARIA {
							if ts, ok := ftags[d]; ok {
								for _, t := range ts {
									if t == "update" {
										if db.Dialect == POSTGRES {
											trigs += "ON " + to_table_name
										}
										err := ExecSQL(db.Name, trigs)
										if klog.CheckError(err) {
											return
										}
									}
								}
							}
						}
						if Debug {
							klog.Printf("%s\n", sst)
							klog.Printf("%s\n", statement)
							klog.Printf("%s\n", trigs)
						}
						_, err := conn.Exec(sst)
						if klog.CheckError(err) {
							klog.Printf("%s\n", sst)
							return
						}
						_, err = conn.Exec(statement)
						if err != nil {
							temp := to_table_name + "_temp"
							_, err := autoMigrate[T](db, temp, true)
							if klog.CheckError(err) {
								return
							}
							cls := strings.Join(fields, ",")
							_, err = conn.Exec("INSERT INTO " + temp + " SELECT " + cls + " FROM " + to_table_name)
							if klog.CheckError(err) {
								return
							}
							_, err = Table(to_table_name).Database(db.Name).Drop()
							if klog.CheckError(err) {
								return
							}
							_, err = conn.Exec("ALTER TABLE " + temp + " RENAME TO " + to_table_name)
							if klog.CheckError(err) {
								return
							}
						}
						klog.Printfs("grDone, '%s' removed from '%s'\n", d, to_table_name)
						os.Exit(0)
					}
				} else {
					conn := db.Conn
					if conn != nil {
						// triggers
						if db.Dialect != MYSQL && db.Dialect != MARIA {
							if ts, ok := ftags[d]; ok {
								for _, t := range ts {
									if t == "update" {
										if db.Dialect == POSTGRES {
											trigs += "ON " + to_table_name
										}
										err := ExecSQL(db.Name, trigs)
										if klog.CheckError(err) {
											return
										}
									}
								}
							}
						}
						_, err := conn.Exec(sst)
						if klog.CheckError(err) {
							klog.Printf("%s\n", sst)
							return
						}
						if Debug {
							klog.Printf("%s\n", sst)
							klog.Printf("%s\n", statement)
							klog.Printf("%s\n", trigs)
						}
						_, err = conn.Exec(statement)
						if err != nil {
							temp := to_table_name + "_temp"
							_, err := autoMigrate[T](db, temp, true)
							if klog.CheckError(err) {
								return
							}
							cls := strings.Join(fields, ",")
							_, err = conn.Exec("INSERT INTO " + temp + " SELECT " + cls + " FROM " + to_table_name)
							if klog.CheckError(err) {
								return
							}
							_, err = Table(to_table_name).Database(db.Name).Drop()
							if klog.CheckError(err) {
								return
							}
							_, err = conn.Exec("ALTER TABLE " + temp + " RENAME TO " + to_table_name)
							if klog.CheckError(err) {
								return
							}
						}
						klog.Printfs("grDone, '%s' removed from '%s'\n", d, to_table_name)
						os.Exit(0)
					}
				}
			} else if SliceContains([]string{"generate", "G", "g"}, choice) {
				query := ""
				sst := "DROP INDEX IF EXISTS idx_" + to_table_name + "_" + d + ";"
				trigs := "DROP TRIGGER IF EXISTS " + to_table_name + "_update_trig;"

				if len(databases) > 1 && db.Name == "" {
					ddb := kinput.Input(kinput.Blue, "> There are more than one database connected, enter database name: ")
					conn := GetSQLConnection(ddb)
					if conn != nil {
						// triggers
						if db.Dialect != MYSQL && db.Dialect != MARIA {
							if ts, ok := ftags[d]; ok {
								for _, t := range ts {
									if t == "update" {
										if db.Dialect == POSTGRES {
											trigs += "ON " + to_table_name
										}
										if !strings.HasSuffix(trigs, ";") {
											trigs += ";"
										}
										query += trigs
									}
								}
							}
						}
						if Debug {
							klog.Printf("%s\n", sst)
							klog.Printf("%s\n", statement)
							klog.Printf("%s\n", trigs)
						}
						if !strings.HasSuffix(sst, ";") {
							sst += ";"
						}
						query += sst

						temp := to_table_name + "_temp"
						tempQuery, err := autoMigrate[T](db, temp, false)
						if klog.CheckError(err) {
							return
						}
						query += tempQuery
						if !strings.HasSuffix(tempQuery, ";") {
							tempQuery += ";"
						}
						cls := strings.Join(fields, ",")

						query += "INSERT INTO " + temp + " SELECT " + cls + " FROM " + to_table_name + ";"
						query += "DROP TABLE " + to_table_name + ";"
						query += "ALTER TABLE " + temp + " RENAME TO " + to_table_name + ";"
					}
				} else {
					conn := db.Conn
					if conn != nil {
						// triggers
						if db.Dialect != MYSQL && db.Dialect != MARIA {
							if ts, ok := ftags[d]; ok {
								for _, t := range ts {
									if t == "update" {
										if db.Dialect == POSTGRES {
											trigs += "ON " + to_table_name
										}
										if !strings.HasSuffix(trigs, ";") {
											trigs += ";"
										}
										query += trigs
									}
								}
							}
						}
						if !strings.HasSuffix(sst, ";") {
							sst += ";"
						}
						query += sst
						if Debug {
							klog.Printf("%s\n", sst)
							klog.Printf("%s\n", statement)
							klog.Printf("%s\n", trigs)
						}
						temp := to_table_name + "_temp"
						tempQuery, err := autoMigrate[T](db, temp, false)
						if klog.CheckError(err) {
							return
						}
						query += tempQuery
						cls := strings.Join(fields, ",")

						query += "INSERT INTO " + temp + " SELECT " + cls + " FROM " + to_table_name + ";"
						query += "DROP TABLE " + to_table_name + ";"
						query += "ALTER TABLE " + temp + " RENAME TO " + to_table_name + ";"
					}
				}

				if _, err := os.Stat("migrations"); err != nil {
					err := os.MkdirAll("migrations", os.ModeDir)
					klog.CheckError(err)
				}

				if _, err := os.Stat("migrations/" + fileName); err != nil {
					f, err := os.Create("migrations/" + fileName)
					if klog.CheckError(err) {
						return
					}
					_, err = f.WriteString(query)
					if klog.CheckError(err) {
						f.Close()
						return
					}
					f.Close()
					fmt.Printf(klog.Green, "migrations/"+fileName+" created")
				} else {
					f, err := os.Open("migrations/" + fileName)
					if klog.CheckError(err) {
						return
					}
					_, err = f.WriteString(query)
					if klog.CheckError(err) {
						f.Close()
						return
					}
					f.Close()
					fmt.Printf(klog.Green, "migrations/"+fileName+" created")
				}
			} else {
				fmt.Printf(klog.Green, "Nothing changed.")
			}
		}
	} else if len(cols) < len(fields) { // missing column db
		for _, d := range diff {
			fileName := "add_" + to_table_name + "_" + d + ".sql"
			if v, ok := ftags[d]; ok && v[0] == "-" || d == pk {
				continue
			}
			if _, err := os.Stat("migrations/" + fileName); err == nil {
				continue
			}
			fmt.Println(" ")
			klog.Printfs("⚠️ column '%s' is missing from the database table '%s'\n", d, to_table_name)
			choice, err := kinput.String(kinput.Yellow, "> do you want to add '"+d+"' to the database ?, you can also generate the query using 'g' (Y/g/n):")
			klog.CheckError(err)
			statement := "ALTER TABLE " + to_table_name + " ADD " + d + " "
			if ty, ok := ftypes[d]; ok {
				res := map[string]string{}
				fkeys := []string{}
				indexes := []string{}
				mindexes := map[string]string{}
				uindexes := map[string]string{}
				var trigs []string
				mi := &migrationInput{
					table:    to_table_name,
					dialect:  db.Dialect,
					fName:    d,
					fType:    ty,
					fTags:    &ftags,
					fKeys:    &fkeys,
					res:      &res,
					indexes:  &indexes,
					mindexes: &mindexes,
					uindexes: &uindexes,
				}
				ty = strings.ToLower(ty)
				switch {
				case strings.Contains(ty, "str"):
					handleMigrationString(mi)
					var s string
					var fkey string
					if v, ok := res[d]; ok {
						s = v
						if strings.Contains(v, "UNIQUE") {
							s = strings.ReplaceAll(v, "UNIQUE", "")
							uindexes[d] = d
						}
					} else {
						s = "VARCHAR(255)"
					}
					for _, fk := range fkeys {
						r := strings.Index(fk, "REFERENCE")
						fkey = fk[r:]
					}
					if fkey != "" {
						s += " " + fkey
					}
					statement += s
				case strings.Contains(ty, "bool"):
					handleMigrationBool(mi)
					var s string
					var fkey string
					if v, ok := res[d]; ok {
						s = v
						if !strings.Contains(v, "DEFAULT 0") {
							s += " DEFAULT 0"
						}
						if strings.Contains(v, "UNIQUE") {
							s = strings.ReplaceAll(v, "UNIQUE", "")
							uindexes[d] = d
						}
					} else {
						s = "INTEGER NOT NULL CHECK (" + d + " IN (0, 1)) DEFAULT 0"
					}
					for _, fk := range fkeys {
						r := strings.Index(fk, "REFERENCE")
						fkey = fk[r:]
					}
					if fkey != "" {
						s += " " + fkey
					}
					statement += s
				case strings.Contains(ty, "int"):
					handleMigrationInt(mi)
					var s string
					var fkey string
					if v, ok := res[d]; ok {
						s = v
						if strings.Contains(v, "UNIQUE") {
							s = strings.ReplaceAll(v, "UNIQUE", "")
							uindexes[d] = d
						}
					} else {
						s = "INTEGER"
					}
					for _, fk := range fkeys {
						r := strings.Index(fk, "REFERENCE")
						fkey = fk[r:]
					}
					if fkey != "" {
						s += " " + fkey
					}
					statement += s
				case strings.Contains(ty, "floa"):
					handleMigrationFloat(mi)
					var s string
					var fkey string
					if v, ok := res[d]; ok {
						s = v
						if strings.Contains(v, "UNIQUE") {
							s = strings.ReplaceAll(v, "UNIQUE", "")
							uindexes[d] = d
						}
					} else {
						s = "DECIMAL(5,2)"
					}
					for _, fk := range fkeys {
						r := strings.Index(fk, "REFERENCE")
						fkey = fk[r:]
					}
					if fkey != "" {
						s += " " + fkey
					}
					statement += s
				case strings.Contains(ty, "time"):
					handleMigrationTime(mi)
					var s string
					var fkey string
					if v, ok := res[d]; ok {
						s = v
						if strings.Contains(v, "UNIQUE") {
							s = strings.ReplaceAll(v, "UNIQUE", "")
							uindexes[d] = d
						}
					} else {
						if strings.Contains(db.Dialect, SQLITE) {
							s = "TEXT"
						} else {
							s = "TIMESTAMP"
						}
					}
					s = strings.ToLower(s)
					if strings.Contains(s, "default") {
						sp := strings.Split(s, " ")
						s = strings.Join(sp[:len(sp)-2], " ")
					}
					if strings.Contains(s, "not null") {
						s = strings.ReplaceAll(s, "not null", "")
					}
					for _, fk := range fkeys {
						r := strings.Index(fk, "REFERENCE")
						fkey = fk[r:]
					}
					if fkey != "" {
						s += " " + fkey
					}
					statement += s

					// triggers
					if db.Dialect != MYSQL && db.Dialect != MARIA {
						if ts, ok := ftags[d]; ok {
							for _, t := range ts {
								if t == "update" {
									v := checkUpdatedAtTrigger(db.Dialect, to_table_name, d, pk)
									for _, stmts := range v {
										trigs = stmts
									}
								}
							}
						}
					}
				default:
					klog.Printf("case not handled:%s\n", ty)
					return
				}

				statIndexes, mstatIndexes, ustatIndexes := handleIndexes(to_table_name, d, indexes, mi)

				if SliceContains([]string{"yes", "Y", "y"}, choice) {
					if len(databases) > 1 && db.Name == "" {
						ddb := kinput.Input(kinput.Blue, "> There are more than one database connected, database name:")
						conn := GetSQLConnection(ddb)
						if conn != nil {
							_, err := conn.Exec(statement)
							if klog.CheckError(err) {
								klog.Printf("%s\n", statement)
								return
							}
							if len(trigs) > 0 {
								for _, st := range trigs {
									_, err := conn.Exec(st)
									if klog.CheckError(err) {
										klog.Printf("triggers:%s\n", st)
										return
									}
								}
							}

							if statIndexes != "" {
								_, err := conn.Exec(statIndexes)
								if klog.CheckError(err) {
									klog.Printf("%s\n", statIndexes)
									return
								}
							}
							if mstatIndexes != "" {
								_, err := conn.Exec(mstatIndexes)
								if klog.CheckError(err) {
									klog.Printf("%s\n", mstatIndexes)
									return
								}
							}
							if ustatIndexes != "" {
								_, err := conn.Exec(ustatIndexes)
								if klog.CheckError(err) {
									klog.Printf("%s\n", ustatIndexes)
									return
								}
							}
							if Debug {
								if statement != "" {
									klog.Printfs("ylstatement: %s\n", statement)
								}
								if statIndexes != "" {
									klog.Printfs("ylstatIndexes: %s\n", statIndexes)
								}
								if mstatIndexes != "" {
									klog.Printfs("ylmstatIndexes: %s\n", mstatIndexes)
								}
								if ustatIndexes != "" {
									klog.Printfs("ylustatIndexes: %s\n", ustatIndexes)
								}
								if len(trigs) > 0 {
									klog.Printfs("yltriggers: %v\n", trigs)
								}
							}
							klog.Printfs("grDone, '%s' added to '%s', you may want to restart your server\n", d, to_table_name)
						}
					} else {
						conn := GetSQLConnection(db.Name)
						if conn != nil {
							_, err := conn.Exec(statement)
							if klog.CheckError(err) {
								klog.Printf("%s\n", statement)
								return
							}
							if len(trigs) > 0 {
								for _, st := range trigs {
									_, err := conn.Exec(st)
									if klog.CheckError(err) {
										klog.Printf("triggers:%s\n", st)
										return
									}
								}
							}
							if statIndexes != "" {
								_, err := conn.Exec(statIndexes)
								if klog.CheckError(err) {
									klog.Printf("%s\n", statIndexes)
									return
								}
							}
							if mstatIndexes != "" {
								_, err := conn.Exec(mstatIndexes)
								if klog.CheckError(err) {
									klog.Printf("%s\n", mstatIndexes)
									return
								}
							}
							if ustatIndexes != "" {
								_, err := conn.Exec(ustatIndexes)
								if klog.CheckError(err) {
									klog.Printf("%s\n", ustatIndexes)
									return
								}
							}
							if Debug {
								if statement != "" {
									klog.Printfs("ylstatement: %s\n", statement)
								}
								if statIndexes != "" {
									klog.Printfs("ylstatIndexes: %s\n", statIndexes)
								}
								if mstatIndexes != "" {
									klog.Printfs("ylmstatIndexes: %s\n", mstatIndexes)
								}
								if ustatIndexes != "" {
									klog.Printfs("ylustatIndexes: %s\n", ustatIndexes)
								}
								if len(trigs) > 0 {
									klog.Printfs("yltriggers: %v\n", trigs)
								}
							}
							klog.Printfs("grDone, '%s' added to '%s', you may want to restart your server\n", d, to_table_name)
						}
					}
				} else if SliceContains([]string{"generate", "G", "g"}, choice) {
					query := ""

					if len(databases) > 1 && db.Name == "" {
						ddb := kinput.Input(kinput.Blue, "> There are more than one database connected, database name:")
						conn := GetSQLConnection(ddb)
						if conn != nil {
							if !strings.HasSuffix(statement, ";") {
								statement += ";"
							}
							query += statement
							if len(trigs) > 0 {
								for _, st := range trigs {
									if !strings.HasSuffix(st, ";") {
										st += ";"
									}
									query += st
								}
							}

							if statIndexes != "" {
								if !strings.HasSuffix(statIndexes, ";") {
									statIndexes += ";"
								}
								query += statIndexes
							}
							if mstatIndexes != "" {
								if !strings.HasSuffix(mstatIndexes, ";") {
									mstatIndexes += ";"
								}
								query += mstatIndexes
							}
							if ustatIndexes != "" {
								if !strings.HasSuffix(ustatIndexes, ";") {
									ustatIndexes += ";"
								}
								query += ustatIndexes
							}
							if Debug {
								if statement != "" {
									klog.Printfs("ylstatement: %s\n", statement)
								}
								if statIndexes != "" {
									klog.Printfs("ylstatIndexes: %s\n", statIndexes)
								}
								if mstatIndexes != "" {
									klog.Printfs("ylmstatIndexes: %s\n", mstatIndexes)
								}
								if ustatIndexes != "" {
									klog.Printfs("ylustatIndexes: %s\n", ustatIndexes)
								}
								if len(trigs) > 0 {
									klog.Printfs("yltriggers: %v\n", trigs)
								}
							}
						}
					} else {
						conn := GetSQLConnection(db.Name)
						if conn != nil {
							if !strings.HasSuffix(statement, ";") {
								statement += ";"
							}
							query += statement
							if len(trigs) > 0 {
								for _, st := range trigs {
									if !strings.HasSuffix(st, ";") {
										st += ";"
									}
									query += st
								}
							}
							if statIndexes != "" {
								if !strings.HasSuffix(statIndexes, ";") {
									statIndexes += ";"
								}
								query += statIndexes
							}
							if mstatIndexes != "" {
								if !strings.HasSuffix(mstatIndexes, ";") {
									mstatIndexes += ";"
								}
								query += mstatIndexes
							}
							if ustatIndexes != "" {
								if !strings.HasSuffix(ustatIndexes, ";") {
									ustatIndexes += ";"
								}
								query += ustatIndexes
							}
							if Debug {
								if statement != "" {
									klog.Printfs("ylstatement: %s\n", statement)
								}
								if statIndexes != "" {
									klog.Printfs("ylstatIndexes: %s\n", statIndexes)
								}
								if mstatIndexes != "" {
									klog.Printfs("ylmstatIndexes: %s\n", mstatIndexes)
								}
								if ustatIndexes != "" {
									klog.Printfs("ylustatIndexes: %s\n", ustatIndexes)
								}
								if len(trigs) > 0 {
									klog.Printfs("yltriggers: %v\n", trigs)
								}
							}
							klog.Printfs("grDone, '%s' added to '%s', you may want to restart your server\n", d, to_table_name)
						}
					}

					if _, err := os.Stat("migrations"); err != nil {
						err := os.MkdirAll("migrations", os.ModeDir)
						klog.CheckError(err)
					}

					if _, err := os.Stat("migrations/" + fileName); err != nil {
						f, err := os.Create("migrations/" + fileName)
						if klog.CheckError(err) {
							return
						}
						_, err = f.WriteString(query)
						if klog.CheckError(err) {
							f.Close()
							return
						}
						f.Close()
						klog.Printfs("grmigrations/%s created\n", fileName)
					} else {
						f, err := os.Open("migrations/" + fileName)
						if klog.CheckError(err) {
							return
						}
						_, err = f.WriteString(query)
						if klog.CheckError(err) {
							f.Close()
							return
						}
						f.Close()
						klog.Printfs("grmigrations/%s created\n", fileName)
					}
				} else {
					klog.Printfs("grNothing changed\n")
				}
			} else {
				klog.Printf("case not handled:%s %s \n", ty, ftypes[d])
			}
		}
	}
}

func handleIndexes(to_table_name, colName string, indexes []string, mi *migrationInput) (statIndexes string, mstatIndexes string, ustatIndexes string) {
	if len(indexes) > 0 {
		if len(indexes) > 1 {
			klog.Printf("%s cannot have more than 1 index\n", mi.fName)
		} else {
			ff := strings.ReplaceAll(colName, "DESC", "")
			statIndexes = fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s)", to_table_name, ff, to_table_name, indexes[0])
		}
	}

	if len(*mi.mindexes) > 0 {
		if len(*mi.mindexes) > 1 {
			klog.Printf("%s cannot have more than 1 multiple indexes\n", mi.fName)
		} else {
			if v, ok := (*mi.mindexes)[mi.fName]; ok {
				ff := strings.ReplaceAll(colName, "DESC", "")
				mstatIndexes = fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s)", to_table_name, ff, to_table_name, colName+","+v)
			}
		}
	}

	if len(*mi.uindexes) > 0 {
		if len(*mi.uindexes) > 1 {
			klog.Printf("%s cannot have more than 1 multiple indexes", mi.fName)
		} else {
			if v, ok := (*mi.uindexes)[mi.fName]; ok {
				sp := strings.Split(v, ",")
				for i := range sp {
					if sp[i][0] == 'I' {
						sp[i] = "LOWER(" + sp[i][1:] + ")"
					}
				}
				if len(sp) > 0 {
					v = strings.Join(sp, ",")
				}
				ustatIndexes = fmt.Sprintf("CREATE UNIQUE INDEX idx_%s_%s ON %s (%s)", to_table_name, colName, to_table_name, v)
			}
		}
	}
	return statIndexes, mstatIndexes, ustatIndexes
}

// handleRename handle sync with db when renaming fields struct
func handleRename(to_table_name string, fields, cols, diff []string, db *DatabaseEntity, ftags map[string][]string, pk string) {
	// rename field
	old := []string{}
	new := []string{}
	if len(fields) == len(cols) && len(diff)%2 == 0 && len(diff) > 0 {
		for _, d := range diff {
			if v, ok := ftags[d]; ok && v[0] == "-" || d == pk {
				continue
			}
			if !SliceContains(cols, d) { // d is new
				new = append(new, d)
			} else { // d is old
				old = append(old, d)
			}
		}
	}
	if len(new) > 0 && len(new) == len(old) {
		if len(new) == 1 {
			choice := kinput.Input(kinput.Yellow, "⚠️ you renamed '"+old[0]+"' to '"+new[0]+"', execute these changes to db ? (Y/n):")
			if SliceContains([]string{"yes", "Y", "y"}, choice) {
				if tags, ok := ftags[new[0]]; ok {
					if SliceContains(tags, "update") {
						klog.Printfs("rdcannot rename update_at field, triggers must be renamed\n")
						return
					}
				}
				statement := "ALTER TABLE " + to_table_name + " RENAME COLUMN " + old[0] + " TO " + new[0]
				if len(databases) > 1 && db.Name == "" {
					ddb := kinput.Input(kinput.Blue, "> There are more than one database connected, database name:")
					conn := GetSQLConnection(ddb)
					if conn != nil {
						if Debug {
							klog.Printf("ylstatement:%s\n", statement)
						}
						_, err := conn.Exec(statement)
						if klog.CheckError(err) {
							klog.Printf("statement:%s\n", statement)
							return
						}
						klog.Printfs("grDone, '%s' has been changed to %s\n", old[0], new[0])
					}
				} else {
					conn := db.Conn
					if conn != nil {
						if Debug {
							klog.Printf("statement:%s\n", statement)
						}
						_, err := conn.Exec(statement)
						if klog.CheckError(err) {
							return
						}
						klog.Printfs("grDone, '%s' has been changed to %s\n", old[0], new[0])
					}
				}
			} else {
				klog.Printfs("grNothing changed\n")
			}
		} else {
			for _, n := range new {
				for _, o := range old {
					if strings.HasPrefix(n, o) || strings.HasPrefix(o, n) {
						choice := kinput.Input(kinput.Yellow, "⚠️ you renamed '"+o+"' to '"+n+"', execute these changes to db ? (Y/n):")
						if SliceContains([]string{"yes", "Y", "y"}, choice) {
							statement := "ALTER TABLE " + to_table_name + " RENAME COLUMN " + o + " TO " + n
							if len(databases) > 1 && db.Name == "" {
								ddb := kinput.Input(kinput.Blue, "> There are more than one database connected, database name:")
								conn := GetSQLConnection(ddb)
								if conn != nil {
									if Debug {
										klog.Printf("statement:%s\n", statement)
									}
									_, err := conn.Exec(statement)
									if klog.CheckError(err) {
										klog.Printf("statement:%s\n", statement)
										return
									}
								}
							} else {
								if Debug {
									klog.Printf("statement:%s\n", statement)
								}
								conn := GetSQLConnection(db.Name)
								if conn != nil {
									_, err := conn.Exec(statement)
									if klog.CheckError(err) {
										klog.Printf("statement:%s\n", statement)
										return
									}
								}
							}
							klog.Printfs("grDone, '%s' has been changed to %s\n", o, n)
						}
					}
				}
			}
		}
	}
}

func getConstraints(db *DatabaseEntity, tableName string) map[string][]string {
	res := map[string][]string{}
	switch db.Dialect {
	case SQLITE:
		st := "select sql from sqlite_master where type='table' and name='" + tableName + "';"
		d, err := Query(db.Name, st)
		if klog.CheckError(err) {
			return nil
		}
		sqlStat := d[0]["sql"]
		if _, after, found := strings.Cut(sqlStat.(string), "("); found {
			lines := strings.Split(after[:len(after)-1], ",")
			for _, l := range lines {
				sp := strings.Split(l, " ")
				if len(sp) > 1 && sp[1] != "" {
					col := sp[0]
					tags := sp[1:]
					if col != "" && len(tags) > 1 {
						for _, t := range tags {
							switch t {
							case "PRIMARY", "PRIMARY KEY":
								res[col] = append(res[col], "pkey")
							case "NOT NULL", "NULL":
								res[col] = append(res[col], "notnull")
							case "FOREIGN", "FOREIGN KEY":
								res[col] = append(res[col], "fkey")
							case "CHECK":
								res[col] = append(res[col], "chk")
							case "UNIQUE":
								res[col] = append(res[col], "key")
							default:
								if t == "KEY" && col == "FOREIGN" {
									col := tags[1][1 : len(tags[1])-1]
									res[col] = append(res[col], "fkey")
								}
							}
						}
					}
				}
			}
		}
	case POSTGRES, MYSQL, MARIA:
		st := "select table_name,constraint_type,constraint_name from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where table_name='" + tableName + "';"
		d, err := Query(db.Name, st)
		if !klog.CheckError(err) {
			for _, dd := range d {
				klog.Printf("gr%s\n", dd)
				switch {
				case strings.HasPrefix(dd["constraint_name"].(string), "chk_"):
					ln := len("chk_") + len(tableName) + 1
					col := dd["constraint_name"].(string)[ln:]
					res[col] = append(res[col], "chk")
				case strings.HasSuffix(dd["constraint_name"].(string), "_pkey"):
					sp := strings.Split(dd["constraint_name"].(string), "_")
					sp = sp[:len(sp)-1]
					col := strings.Join(sp, "_")
					res[col] = append(res[col], "pkey")
				case strings.HasSuffix(dd["constraint_name"].(string), "_fkey"):
					if constraintName, ok := dd["constraint_name"].(string); ok {
						sp := strings.Split(constraintName, "_")
						table := sp[0]
						if table != tableName {
							for i := 2; true; i++ {
								table = strings.Join(sp[0:i], "_")
								if table == tableName {
									break
								}
							}

						}
						ln := len(table) + 2
						col := constraintName[ln:(len(constraintName) - len("_fkey"))]
						res[col] = append(res[col], "fkey")

					}
				case strings.HasSuffix(dd["constraint_name"].(string), "_key"):
					if constraintName, ok := dd["constraint_name"].(string); ok {
						// users_email_key
						sp := strings.Split(constraintName, "_")
						table := sp[0]
						if table != tableName {
							for i := 2; true; i++ {
								table = strings.Join(sp[0:i], "_")
								if table == tableName {
									break
								}
							}

						}
						ln := len(table) + 2
						col := constraintName[ln : len(constraintName)-len("_key")]
						res[col] = append(res[col], "key")

					}
				default:
				}
			}
		}
	}
	return res
}

func getTableName[T comparable]() string {
	if v, ok := mModelTablename[*new(T)]; ok {
		return v
	} else {
		return ""
	}
}

// getStructInfos very useful to access all struct fields data using reflect package
func getStructInfos[T comparable](strctt *T) (fields []string, fValues map[string]any, fTypes map[string]string, fTags map[string][]string) {
	fields = []string{}
	fValues = map[string]any{}
	fTypes = map[string]string{}
	fTags = map[string][]string{}

	s := reflect.ValueOf(strctt).Elem()
	typeOfT := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		fname := typeOfT.Field(i).Name
		fname = ToSnakeCase(fname)
		fvalue := f.Interface()
		ftype := f.Type().Name()

		fields = append(fields, fname)
		fTypes[fname] = ftype
		fValues[fname] = fvalue
		if ftag, ok := typeOfT.Field(i).Tag.Lookup("korm"); ok {
			tags := strings.Split(ftag, ";")
			fTags[fname] = tags
		}
	}
	return fields, fValues, fTypes, fTags
}

func adaptPlaceholdersToDialect(query *string, dialect string) *string {
	if strings.Contains(*query, "?") && (dialect == POSTGRES || dialect == SQLITE) {
		split := strings.Split(*query, "?")
		counter := 0
		for i := range split {
			if i < len(split)-1 {
				counter++
				split[i] = split[i] + "$" + strconv.Itoa(counter)
			}
		}
		*query = strings.Join(split, "")
	}
	return query
}

func handleCache(data map[string]any) {
	switch data["type"] {
	case "create", "delete", "update":
		go func() {
			cachesAllM.Flush()
			cachesAllS.Flush()
			cachesOneM.Flush()
			cachesOneS.Flush()
		}()
	case "drop":
		go func() {
			cacheGetAllTables.Flush()
			cachesAllM.Flush()
			cachesAllS.Flush()
			cachesOneM.Flush()
			cachesOneS.Flush()
		}()
	case "clean":
		go func() {
			cacheGetAllTables.Flush()
			cachesAllM.Flush()
			cachesAllS.Flush()
			cachesOneM.Flush()
			cachesOneS.Flush()
		}()
	default:
		klog.Printf("CACHE DB: default case triggered %v \n", data)
	}
}

func GenerateUUID() (string, error) {
	var uuid [16]byte
	_, err := io.ReadFull(rand.Reader, uuid[:])
	if err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant is 10
	var buf [36]byte
	encodeHex(buf[:], uuid)
	return string(buf[:]), nil
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

func RunEvery(t time.Duration, function any) {
	//Usage : go RunEvery(2 * time.Second,func(){})
	fn, ok := function.(func())
	if !ok {
		fmt.Println("ERROR : fn is not a function")
		return
	}

	fn()
	c := time.NewTicker(t)

	for range c.C {
		fn()
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

func Difference[T comparable](slice1 []T, slice2 []T) []T {
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

func SliceRemove[T comparable](slice *[]T, elemsToRemove ...T) {
	for i, elem := range *slice {
		for _, e := range elemsToRemove {
			if e == elem {
				*slice = append((*slice)[:i], (*slice)[i+1:]...)
			}
		}
	}
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func StringContains(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
