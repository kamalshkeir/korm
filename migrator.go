package korm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kstrct"
)

var migrationAutoCheck = true

func checkUpdatedAtTrigger(dialect, tableName, col string) map[string][]string {
	triggers := map[string][]string{}
	t := "datetime('now','localtime')"
	if dialect == "sqlite" {
		st := "CREATE TRIGGER IF NOT EXISTS "
		st += tableName + "_update_trig AFTER UPDATE ON " + tableName
		st += " BEGIN update " + tableName + " SET " + col + " = " + t
		st += " WHERE " + col + " = " + "NEW." + col + ";"
		st += "End;"
		triggers[col] = []string{st}
	} else if dialect == "postgres" {
		st := "CREATE OR REPLACE FUNCTION updated_at_trig() RETURNS trigger AS $$"
		st += " BEGIN NEW." + col + " = now();RETURN NEW;"
		st += "END;$$ LANGUAGE plpgsql;"
		triggers[col] = []string{st}
		trigCreate := "CREATE OR REPLACE TRIGGER " + tableName + "_update_trig"
		trigCreate += " BEFORE UPDATE ON public." + tableName
		trigCreate += " FOR EACH ROW EXECUTE PROCEDURE updated_at_trig();"
		triggers[col] = append(triggers[col], trigCreate)
	} else {
		return nil
	}
	return triggers
}

func autoMigrate[T comparable](db *DatabaseEntity, tableName string, execute bool) (string, error) {
	toReturnstats := []string{}
	dialect := db.Dialect
	s := reflect.ValueOf(new(T)).Elem()
	typeOfT := s.Type()
	mFieldName_Type := map[string]string{}
	mFieldName_Tags := map[string][]string{}
	cols := []string{}
	pk := ""

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		fname := typeOfT.Field(i).Name
		fname = kstrct.ToSnakeCase(fname)
		ftype := f.Type()
		mFieldName_Type[fname] = ftype.Name()
		if ftag, ok := typeOfT.Field(i).Tag.Lookup("korm"); ok {
			tags := strings.Split(ftag, ";")
			for i, tag := range tags {
				tag := strings.TrimSpace(tag)
				if tag == "autoinc" || tag == "pk" {
					pk = fname
				} else if fname == "id" {
					pk = fname
				} else if ftag == "-" {
					continue
				}
				tags[i] = strings.TrimSpace(tags[i])
			}
			mFieldName_Tags[fname] = tags
		} else if ftag, ok := typeOfT.Field(i).Tag.Lookup("kstrct"); ok {
			if ftag == "-" {
				continue
			}
		}
		cols = append(cols, fname)
	}
	if pk == "" {
		cols = append([]string{"id"}, cols...)
		mFieldName_Type["id"] = "uint"
		mFieldName_Tags["id"] = []string{"pk"}
		pk = "id"
	}

	res := map[string]string{}
	fkeys := []string{}
	indexes := []string{}
	mindexes := map[string]string{}
	uindexes := map[string]string{}
	var mi *migrationInput
	for _, fName := range cols {
		if ty, ok := mFieldName_Type[fName]; ok {
			mi = &migrationInput{
				table:    tableName,
				dialect:  dialect,
				fName:    fName,
				fType:    ty,
				fTags:    &mFieldName_Tags,
				fKeys:    &fkeys,
				res:      &res,
				indexes:  &indexes,
				mindexes: &mindexes,
				uindexes: &uindexes,
			}
			switch ty {
			case "int", "uint", "int64", "uint64", "int32", "uint32":
				handleMigrationInt(mi)
			case "bool":
				handleMigrationBool(mi)
			case "string":
				handleMigrationString(mi)
			case "float64", "float32":
				handleMigrationFloat(mi)
			case "Time":
				handleMigrationTime(mi)
			default:
				isM2M := false
				if tags, ok := mFieldName_Tags[fName]; ok {
					for _, tag := range tags {
						if strings.Contains(tag, "m2m") {
							isM2M = true
						}
					}
				}
				if !isM2M {
					klog.Printf("rd%s of type %s not handled\n", fName, ty)
				}
			}
		}
	}
	statement := prepareCreateStatement(tableName, res, fkeys, cols)
	var triggers map[string][]string
	tbFound := false

	// check if table in memory
	for _, t := range db.Tables {
		if t.Name == tableName {
			tbFound = true
			if len(t.Columns) == 0 {
				t.Columns = cols
			}
			if len(t.Tags) == 0 {
				t.Tags = mFieldName_Tags
			}
			if len(t.ModelTypes) == 0 {
				t.Types = mFieldName_Type
			}
		}
	}
	// check for update field to create a trigger
	if db.Dialect != MYSQL && db.Dialect != MARIA {
		for col, tags := range mFieldName_Tags {
			for _, tag := range tags {
				if tag == "update" {
					triggers = checkUpdatedAtTrigger(db.Dialect, tableName, col)
				}
			}
		}
	}

	if !tbFound {
		db.Tables = append(db.Tables, TableEntity{
			Name:       tableName,
			Columns:    cols,
			Tags:       mFieldName_Tags,
			ModelTypes: mFieldName_Type,
			Pk:         pk,
		})
	}
	if Debug {
		klog.Printf("ylstatement:%s\n", statement)
	}

	c, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()
	if execute {
		ress, err := db.Conn.ExecContext(c, statement)
		if err != nil {
			return "", err
		}
		_, err = ress.RowsAffected()
		if err != nil {
			return "", err
		}
	}
	toReturnstats = append(toReturnstats, statement)

	if !strings.HasSuffix(tableName, "_temp") {
		if len(triggers) > 0 {
			for _, stats := range triggers {
				for _, st := range stats {
					if Debug {
						klog.Printfs("trigger updated_at %s: %s\n", tableName, st)
					}
					if execute {
						err := Exec(db.Name, st)
						if klog.CheckError(err) {
							klog.Printfs("rdtrigger updated_at %s: %s\n", tableName, st)
							return "", err
						}
					}
					toReturnstats = append(toReturnstats, st)
				}
			}
		}
		statIndexes := ""
		if len(indexes) > 0 {
			if len(indexes) > 1 {
				klog.Printf("%s cannot have more than 1 index\n", mi.fName)
			} else {
				ff := strings.ReplaceAll(indexes[0], "DESC", "")
				statIndexes = fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s)", tableName, ff, tableName, indexes[0])
			}
		}
		mstatIndexes := ""
		if len(*mi.mindexes) > 0 {
			if len(*mi.mindexes) > 1 {
				klog.Printf("%s cannot have more than 1 multiple indexes\n", mi.fName)
			} else {
				for k, v := range *mi.mindexes {
					ff := strings.ReplaceAll(k, "DESC", "")
					mstatIndexes = fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s)", tableName, ff, tableName, k+","+v)
				}
			}
		}
		ustatIndexes := []string{}
		for col, tagValue := range *mi.uindexes {
			sp := strings.Split(tagValue, ",")
			for i := range sp {
				if sp[i][0] == 'I' && db.Dialect != MYSQL && db.Dialect != MARIA {
					sp[i] = "LOWER(" + sp[i][1:] + ")"
				}
			}
			res := strings.Join(sp, ",")
			ustatIndexes = append(ustatIndexes, fmt.Sprintf("CREATE UNIQUE INDEX idx_%s_%s ON %s (%s)", tableName, col, tableName, res))
		}
		if statIndexes != "" {
			if Debug {
				klog.Printfs("%s\n", statIndexes)
			}
			if execute {
				_, err := db.Conn.Exec(statIndexes)
				if klog.CheckError(err) {
					klog.Printfs("rdindexes: %s\n", statIndexes)
					return "", err
				}
			}

			toReturnstats = append(toReturnstats, statIndexes)
		}
		if mstatIndexes != "" {
			if Debug {
				klog.Printfs("mindexes: %s\n", mstatIndexes)
			}
			if execute {
				_, err := db.Conn.Exec(mstatIndexes)
				if klog.CheckError(err) {
					klog.Printfs("rdmindexes: %s\n", mstatIndexes)
					return "", err
				}
			}

			toReturnstats = append(toReturnstats, mstatIndexes)
		}
		if len(ustatIndexes) > 0 {
			for i := range ustatIndexes {
				if Debug {
					klog.Printfs("uindexes: %s\n", ustatIndexes[i])
				}
				if execute {
					_, err := db.Conn.Exec(ustatIndexes[i])
					if klog.CheckError(err) {
						klog.Printfs("rduindexes: %s\n", ustatIndexes)
						return "", err
					}
				}
				toReturnstats = append(toReturnstats, ustatIndexes[i])
			}
		}
	}
	if execute && Debug {
		klog.Printfs("gr %s migrated\n", tableName)
	}
	toReturnQuery := strings.Join(toReturnstats, ";")
	return toReturnQuery, nil
}

func AutoMigrate[T comparable](tableName string, dbName ...string) error {
	if _, ok := mModelTablename[*new(T)]; !ok {
		mModelTablename[*new(T)] = tableName
	}
	var db *DatabaseEntity
	var err error
	dbname := ""
	if len(dbName) == 1 {
		dbname = dbName[0]
		db, err = GetMemoryDatabase(dbname)
		if err != nil || db == nil {
			return errors.New("database not found")
		}
	} else if len(dbName) == 0 {
		dbname = databases[0].Name
		db, err = GetMemoryDatabase(dbname)
		if err != nil || db == nil {
			return errors.New("database not found")
		}
	} else {
		return errors.New("cannot migrate more than one database at the same time")
	}

	tbFoundDB := false
	tables := GetAllTables(dbname)
	for _, t := range tables {
		if t == tableName {
			tbFoundDB = true
		}
	}

	tbFoundLocal := false
	if len(db.Tables) == 0 {
		if tbFoundDB && migrationAutoCheck {
			// found db not local
			linkModel[T](tableName, db)
			return nil
		} else {
			// not db and not local
			_, err := autoMigrate[T](db, tableName, true)
			if klog.CheckError(err) {
				return err
			}
			return nil
		}
	} else {
		// db have tables
		for _, t := range db.Tables {
			if t.Name == tableName {
				tbFoundLocal = true
			}
		}
	}

	if migrationAutoCheck && (tbFoundDB || tbFoundLocal) {
		linkModel[T](tableName, db)
		return nil
	} else {
		_, err := autoMigrate[T](db, tableName, true)
		if klog.CheckError(err) {
			return err
		}
	}

	return nil
}

type migrationInput struct {
	table    string
	dialect  string
	fName    string
	fType    string
	fTags    *map[string][]string
	fKeys    *[]string
	res      *map[string]string
	indexes  *[]string
	mindexes *map[string]string
	uindexes *map[string]string
}

func handleMigrationInt(mi *migrationInput) {
	primary, index, autoinc, notnull, defaultt, checks, unique := "", "", "", "", "", []string{}, ""
	tags := (*mi.fTags)[mi.fName]
	if len(tags) == 1 && tags[0] == "-" {
		(*mi.res)[mi.fName] = ""
		return
	}
	for _, tag := range tags {
		if !strings.Contains(tag, ":") {
			switch tag {
			case "autoinc", "pk":
				switch mi.dialect {
				case SQLITE, "":
					autoinc = "INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"
				case POSTGRES:
					autoinc = "SERIAL NOT NULL PRIMARY KEY"
				case MYSQL, MARIA:
					autoinc = "INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT"
				default:
					klog.Printf("dialect can be sqlite, postgres or mysql only, not %s\n", mi.dialect)
				}
			case "notnull":
				notnull = "NOT NULL"
			case "index", "+index", "index+":
				*mi.indexes = append(*mi.indexes, mi.fName)
			case "-index", "index-":
				*mi.indexes = append(*mi.indexes, mi.fName+" DESC")
			case "unique":
				unique = " UNIQUE"
			case "default":
				defaultt = " DEFAULT 0"
			default:
				klog.Printf("%s not handled for migration int\n", tag)
			}
		} else {
			sp := strings.Split(tag, ":")
			tg := sp[0]
			switch tg {
			case "default":
				defaultt = " DEFAULT " + sp[1]
			case "fk":
				ref := strings.Split(sp[1], ".")
				if len(ref) == 2 {
					fkey := "FOREIGN KEY (" + mi.fName + ") REFERENCES " + ref[0] + "(" + ref[1] + ")"
					if len(sp) > 2 {
						switch sp[2] {
						case "cascade":
							fkey += " ON DELETE CASCADE"
						case "donothing", "noaction":
							fkey += " ON DELETE NO ACTION"
						case "setnull", "null":
							fkey += " ON DELETE SET NULL"
						case "setdefault", "default":
							fkey += " ON DELETE SET DEFAULT"
						default:
							klog.Printf("rdfk %s not handled\n", sp[2])
						}
						if len(sp) > 3 {
							switch sp[3] {
							case "cascade":
								fkey += " ON UPDATE CASCADE"
							case "donothing", "noaction":
								fkey += " ON UPDATE NO ACTION"
							case "setnull", "null":
								fkey += " ON UPDATE SET NULL"
							case "setdefault", "default":
								fkey += " ON UPDATE SET DEFAULT"
							default:
								klog.Printf("rdfk %s not handled\n", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					klog.Printf("allowed options cascade/donothing/noaction\n")
				}
			case "check":
				if strings.Contains(strings.ToLower(sp[1]), "len") {
					switch mi.dialect {
					case SQLITE, "":
						sp[1] = strings.Replace(strings.ToLower(sp[1]), "len", "length", -1)
					case POSTGRES, MYSQL, MARIA:
						sp[1] = strings.Replace(strings.ToLower(sp[1]), "len", "char_length", -1)
					default:
						klog.Printf("check not handled for dialect:%s\n", mi.dialect)
					}
				}
				checks = append(checks, strings.TrimSpace(sp[1]))
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						klog.Printf("mindex not working for %s %v \n", mi.fName, sp[1])
					}
				} else {
					(*mi.mindexes)[mi.fName] = sp[1]
				}
			case "uindex":
				if v, ok := (*mi.uindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.uindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.uindexes)[mi.fName] += "," + sp[1]
					} else {
						klog.Printf("mindex not working for %s %v \n", mi.fName, sp[1])
					}
				} else {
					(*mi.uindexes)[mi.fName] = sp[1]
				}
			default:
				klog.Printf("MIGRATION INT: not handled %s for %s , field: %s\n", sp[0], tag, mi.fName)
			}
		}
	}

	if autoinc != "" {
		// integer auto increment
		(*mi.res)[mi.fName] = autoinc
	} else {
		// integer normal
		(*mi.res)[mi.fName] = "INTEGER"
		if primary != "" {
			(*mi.res)[mi.fName] += primary
		} else {
			if notnull != "" {
				(*mi.res)[mi.fName] += notnull
			}
		}
		if unique != "" {
			(*mi.res)[mi.fName] += unique
		}
		if index != "" {
			(*mi.res)[mi.fName] += index
		}
		if defaultt != "" {
			(*mi.res)[mi.fName] += defaultt
		}
		if len(checks) > 0 {
			joined := strings.TrimSpace(strings.Join(checks, " AND "))
			(*mi.res)[mi.fName] += " CHECK(" + joined + ")"
		}
	}
}

func handleMigrationBool(mi *migrationInput) {
	defaultt := ""

	tags := (*mi.fTags)[mi.fName]
	if len(tags) == 1 && tags[0] == "-" {
		(*mi.res)[mi.fName] = ""
		return
	} else if len(tags) == 0 {
		(*mi.res)[mi.fName] = "INTEGER NOT NULL CHECK (" + mi.fName + " IN (0, 1))"
		return
	}
	for _, tag := range tags {
		if strings.Contains(tag, ":") {
			sp := strings.Split(tag, ":")
			switch sp[0] {
			case "default":
				if sp[1] != "" {
					if sp[1] == "true" {
						defaultt = " DEFAULT 1"
					} else if sp[1] == "false" {
						defaultt = " DEFAULT 0"
					} else {
						defaultt = " DEFAULT " + sp[1]
					}
				} else {
					defaultt = " DEFAULT 0"
				}
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						klog.Printf("mindex not working for %s %s \n", mi.fName, sp[1])
					}
				} else {
					(*mi.mindexes)[mi.fName] = sp[1]
				}
			case "fk":
				ref := strings.Split(sp[1], ".")
				if len(ref) == 2 {
					fkey := "FOREIGN KEY(" + mi.fName + ") REFERENCES " + ref[0] + "(" + ref[1] + ")"
					if len(sp) > 2 {
						switch sp[2] {
						case "cascade":
							fkey += " ON DELETE CASCADE"
						case "donothing", "noaction":
							fkey += " ON DELETE NO ACTION"
						case "setnull", "null":
							fkey += " ON DELETE SET NULL"
						case "setdefault", "default":
							fkey += " ON DELETE SET DEFAULT"
						default:
							klog.Printf("rdfk %s not handled\n", sp[2])
						}
						if len(sp) > 3 {
							switch sp[3] {
							case "cascade":
								fkey += " ON UPDATE CASCADE"
							case "donothing", "noaction":
								fkey += " ON UPDATE NO ACTION"
							case "setnull", "null":
								fkey += " ON UPDATE SET NULL"
							case "setdefault", "default":
								fkey += " ON UPDATE SET DEFAULT"
							default:
								klog.Printf("rdfk %s not handled\n", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					klog.Printf("it should be fk:users.id:cascade/donothing\n")
				}
			default:
				klog.Printf("%s not handled for %s migration bool\n", sp[0], mi.fName)
			}
		} else {
			switch tag {
			case "index", "+index", "index+":
				*mi.indexes = append(*mi.indexes, mi.fName)
			case "-index", "index-":
				*mi.indexes = append(*mi.indexes, mi.fName+" DESC")
			case "default":
				defaultt = " DEFAULT 0"
			default:
				klog.Printf("%s not handled in Migration Bool\n", tag)
			}
		}
	}
	if defaultt != "" {
		(*mi.res)[mi.fName] = "INTEGER NOT NULL" + defaultt + " CHECK (" + mi.fName + " IN (0, 1))"
	}
}

func handleMigrationString(mi *migrationInput) {
	unique, notnull, text, defaultt, size, pk, checks := "", "", "", "", "", "", []string{}
	tags := (*mi.fTags)[mi.fName]
	if len(tags) == 1 && tags[0] == "-" {
		(*mi.res)[mi.fName] = ""
		return
	}
	for _, tag := range tags {
		if !strings.Contains(tag, ":") {
			switch tag {
			case "text":
				text = "TEXT"
			case "notnull":
				notnull = " NOT NULL"
			case "index", "+index", "index+":
				*mi.indexes = append(*mi.indexes, mi.fName)
			case "-index", "index-":
				*mi.indexes = append(*mi.indexes, mi.fName+" DESC")
			case "unique":
				unique = " UNIQUE"
			case "iunique":
				unique = " UNIQUE"
				s := ""
				if mi.dialect != "mysql" {
					s = "I"
				}
				(*mi.uindexes)[mi.fName] = s + mi.fName
			case "default":
				defaultt = " DEFAULT ''"
			default:
				klog.Printf(tag, "not handled for migration string")
			}
		} else {
			sp := strings.Split(tag, ":")
			switch sp[0] {
			case "default":
				defaultt = " DEFAULT " + sp[1]
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						klog.Printf("mindex not working for %s %s \n", mi.fName, sp[1])
					}
				} else {
					(*mi.mindexes)[mi.fName] = sp[1]
				}
			case "uindex":
				if v, ok := (*mi.uindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.uindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.uindexes)[mi.fName] += "," + sp[1]
					} else {
						klog.Printf("mindex not working for %s %s \n", mi.fName, sp[1])
					}
				} else {
					(*mi.uindexes)[mi.fName] = sp[1]
				}
			case "fk":
				ref := strings.Split(sp[1], ".")
				if len(ref) == 2 {
					fkey := "FOREIGN KEY(" + mi.fName + ") REFERENCES " + ref[0] + "(" + ref[1] + ")"
					if len(sp) > 2 {
						switch sp[2] {
						case "cascade":
							fkey += " ON DELETE CASCADE"
						case "donothing", "noaction":
							fkey += " ON DELETE NO ACTION"
						case "setnull", "null":
							fkey += " ON DELETE SET NULL"
						case "setdefault", "default":
							fkey += " ON DELETE SET DEFAULT"
						default:
							klog.Printf("rdfk %s not handled\n", sp[2])
						}
						if len(sp) > 3 {
							switch sp[3] {
							case "cascade":
								fkey += " ON UPDATE CASCADE"
							case "donothing", "noaction":
								fkey += " ON UPDATE NO ACTION"
							case "setnull", "null":
								fkey += " ON UPDATE SET NULL"
							case "setdefault", "default":
								fkey += " ON UPDATE SET DEFAULT"
							default:
								klog.Printf("rdfk %s not handled\n", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					klog.Printf("foreign key should be like fk:table.column:[cascade/donothing]:[cascade/donothing]\n")
				}
			case "size":
				sp := strings.Split(tag, ":")
				if sp[0] == "size" {
					size = sp[1]
				}
			case "check":
				if strings.Contains(strings.ToLower(sp[1]), "len") {
					switch mi.dialect {
					case SQLITE, "":
						sp[1] = strings.Replace(strings.ToLower(sp[1]), "len", "length", -1)
					case POSTGRES, MYSQL, MARIA:
						sp[1] = strings.Replace(strings.ToLower(sp[1]), "len", "char_length", -1)
					default:
						klog.Printf("check not handled for dialect:%s\n", mi.dialect)
					}
				}
				checks = append(checks, strings.TrimSpace(sp[1]))
			default:
				klog.Printf("MIGRATION STRING: not handled %s for %s , field: %s \n", sp[0], tag, mi.fName)
			}
		}
	}

	if text != "" {
		(*mi.res)[mi.fName] = text
	} else {
		if size != "" {
			(*mi.res)[mi.fName] = "VARCHAR(" + size + ")"
		} else {
			(*mi.res)[mi.fName] = "VARCHAR(255)"
		}
	}

	if unique != "" && pk == "" {
		(*mi.res)[mi.fName] += unique
	}
	if notnull != "" && pk == "" {
		(*mi.res)[mi.fName] += notnull
	}
	if pk != "" {
		(*mi.res)[mi.fName] += pk
	}
	if defaultt != "" {
		(*mi.res)[mi.fName] += defaultt
	}
	if len(checks) > 0 {
		joined := strings.TrimSpace(strings.Join(checks, " AND "))
		(*mi.res)[mi.fName] += " CHECK(" + joined + ")"
	}
}

func handleMigrationFloat(mi *migrationInput) {
	mtags := map[string]string{}
	tags := (*mi.fTags)[mi.fName]
	if len(tags) == 0 {
		(*mi.res)[mi.fName] = "DECIMAL(10,5)"
		return
	}
	if len(tags) == 1 && tags[0] == "-" {
		(*mi.res)[mi.fName] = ""
		return
	}
	for _, tag := range tags {
		if !strings.Contains(tag, ":") {
			switch tag {
			case "notnull":
				mtags["notnull"] = " NOT NULL"
			case "index", "+index", "index+":
				*mi.indexes = append(*mi.indexes, mi.fName)
			case "-index", "index-":
				*mi.indexes = append(*mi.indexes, mi.fName+" DESC")
			case "unique":
				(*mi.uindexes)[mi.fName] = " UNIQUE"
			case "default":
				mtags["default"] = " DEFAULT 0.00"
			default:
				klog.Printf("%s not handled for migration float\n", tag)
			}
		} else {
			sp := strings.Split(tag, ":")
			switch sp[0] {
			case "default":
				if sp[1] != "" {
					mtags["default"] = " DEFAULT " + sp[1]
				}
			case "fk":
				ref := strings.Split(sp[1], ".")
				if len(ref) == 2 {
					fkey := "FOREIGN KEY(" + mi.fName + ") REFERENCES " + ref[0] + "(" + ref[1] + ")"
					if len(sp) > 2 {
						switch sp[2] {
						case "cascade":
							fkey += " ON DELETE CASCADE"
						case "donothing", "noaction":
							fkey += " ON DELETE NO ACTION"
						case "setnull", "null":
							fkey += " ON DELETE SET NULL"
						case "setdefault", "default":
							fkey += " ON DELETE SET DEFAULT"
						default:
							klog.Printf("rdfk %s not handled\n", sp[2])
						}
						if len(sp) > 3 {
							switch sp[3] {
							case "cascade":
								fkey += " ON UPDATE CASCADE"
							case "donothing", "noaction":
								fkey += " ON UPDATE NO ACTION"
							case "setnull", "null":
								fkey += " ON UPDATE SET NULL"
							case "setdefault", "default":
								fkey += " ON UPDATE SET DEFAULT"
							default:
								klog.Printf("rdfk %s not handled\n", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					klog.Printf("foreign key should be like fk:table.column:[cascade/donothing]\n")
				}
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						klog.Printf("mindex not working for %s %s \n", mi.fName, sp[1])
					}
				} else {
					(*mi.mindexes)[mi.fName] = sp[1]
				}
			case "uindex":
				if v, ok := (*mi.uindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.uindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.uindexes)[mi.fName] += "," + sp[1]
					} else {
						klog.Printf("mindex not working for %s %s \n", mi.fName, sp[1])
					}
				} else {
					(*mi.uindexes)[mi.fName] = sp[1]
				}
			case "check":
				if strings.Contains(strings.ToLower(sp[1]), "len") {
					switch mi.dialect {
					case SQLITE, "":
						sp[1] = strings.Replace(strings.ToLower(sp[1]), "len", "length", -1)
					case POSTGRES, MYSQL, MARIA:
						sp[1] = strings.Replace(strings.ToLower(sp[1]), "len", "char_length", -1)
					default:
						klog.Printf("check not handled for dialect: %s \n", mi.dialect)
					}
				}
				if v, ok := mtags["check"]; ok && v != "" {
					mtags["check"] += " AND " + strings.TrimSpace(sp[1])
				} else if v == "" {
					mtags["check"] = strings.TrimSpace(sp[1])
				}
			default:
				klog.Printf("MIGRATION FLOAT: not handled %s for %s , field: %s \n", sp[0], tag, mi.fName)
			}
		}

		(*mi.res)[mi.fName] = "DECIMAL(10,5)"
		for k, v := range mtags {
			switch k {
			case "pk":
				(*mi.res)[mi.fName] += v
			case "notnull":
				if _, ok := mtags["pk"]; !ok {
					(*mi.res)[mi.fName] += v
				}
			case "unique":
				if _, ok := mtags["pk"]; !ok {
					(*mi.res)[mi.fName] += v
				}
			case "default":
				(*mi.res)[mi.fName] += v
			case "check":
				(*mi.res)[mi.fName] += " CHECK(" + v + ")"
			default:
				klog.Printf("case %s not handled\n", k)
			}
		}
	}
}

func handleMigrationTime(mi *migrationInput) {
	defaultt, notnull, check := "", "", ""
	tags := (*mi.fTags)[mi.fName]
	if len(tags) == 1 && tags[0] == "-" {
		(*mi.res)[mi.fName] = ""
		return
	}
	for _, tag := range tags {
		if !strings.Contains(tag, ":") {
			switch tag {
			case "now":
				switch mi.dialect {
				case SQLITE, "":
					defaultt = "TEXT NOT NULL DEFAULT (datetime('now','localtime'))"
				case POSTGRES:
					defaultt = "TIMESTAMPTZ  NOT NULL DEFAULT (now())"
				case MYSQL:
					defaultt = "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
				case MARIA:
					defaultt = "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
				default:
					klog.Printf("not handled Time for %s %s \n", mi.fName, mi.fType)
				}
			case "update":
				switch mi.dialect {
				case SQLITE, "":
					defaultt = "TEXT NOT NULL DEFAULT (datetime('now','localtime'))"
				case POSTGRES:
					defaultt = "TIMESTAMP NOT NULL DEFAULT (now())"
				case MYSQL, MARIA:
					defaultt = "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
				default:
					klog.Printf("not handled Time for %s %s \n", mi.fName, mi.fType)
				}
			case "index", "+index", "index+":
				*mi.indexes = append(*mi.indexes, mi.fName)
			case "-index", "index-":
				*mi.indexes = append(*mi.indexes, mi.fName+" DESC")
			default:
				klog.Printf("%s tag not handled for time\n", tag)
			}
		} else {
			sp := strings.Split(tag, ":")
			switch sp[0] {
			case "fk":
				ref := strings.Split(sp[1], ".")
				if len(ref) == 2 {
					fkey := "FOREIGN KEY(" + mi.fName + ") REFERENCES " + ref[0] + "(" + ref[1] + ")"
					if len(sp) > 2 {
						switch sp[2] {
						case "cascade":
							fkey += " ON DELETE CASCADE"
						case "donothing", "noaction":
							fkey += " ON DELETE NO ACTION"
						case "setnull", "null":
							fkey += " ON DELETE SET NULL"
						case "setdefault", "default":
							fkey += " ON DELETE SET DEFAULT"
						default:
							klog.Printf("rdfk %s not handled\n", sp[2])
						}
						if len(sp) > 3 {
							switch sp[3] {
							case "cascade":
								fkey += " ON UPDATE CASCADE"
							case "donothing", "noaction":
								fkey += " ON UPDATE NO ACTION"
							case "setnull", "null":
								fkey += " ON UPDATE SET NULL"
							case "setdefault", "default":
								fkey += " ON UPDATE SET DEFAULT"
							default:
								klog.Printf("rdfk %s not handled\n", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					klog.Printf("wtf ?, it should be fk:users.id:cascade/donothing\n")
				}
			case "check":
				if strings.Contains(strings.ToLower(sp[1]), "len") {
					switch mi.dialect {
					case SQLITE, "":
						sp[1] = strings.Replace(strings.ToLower(sp[1]), "len", "length", -1)
					case POSTGRES, MARIA:
						sp[1] = strings.Replace(strings.ToLower(sp[1]), "len", "char_length", -1)
					default:
						klog.Printf("check not handled for dialect:%s\n", mi.dialect)
					}
				}
				if check != "" {
					check += " AND " + strings.TrimSpace(sp[1])
				} else {
					check += sp[1]
				}
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						klog.Printf("mindex not working for %s %s \n", mi.fName, sp[1])
					}
				} else {
					(*mi.mindexes)[mi.fName] = sp[1]
				}
			default:
				klog.Printf("case %s not handled for time\n", sp[0])
			}
		}
	}
	if defaultt != "" {
		(*mi.res)[mi.fName] = defaultt
	} else {
		if mi.dialect == "" || mi.dialect == SQLITE {
			(*mi.res)[mi.fName] = "TEXT"
		} else {
			(*mi.res)[mi.fName] = "TIMESTAMP"
		}

		if notnull != "" {
			(*mi.res)[mi.fName] += notnull
		}
		if check != "" {
			(*mi.res)[mi.fName] += " CHECK(" + check + ")"
		}
	}
}

func prepareCreateStatement(tbName string, fields map[string]string, fkeys, cols []string) string {
	var strBuilder strings.Builder
	strBuilder.WriteString("CREATE TABLE IF NOT EXISTS ")
	strBuilder.WriteString(tbName + " (")
	for i, col := range cols {
		fName := col
		fType := fields[col]
		if fType == "" {
			continue
		}
		reste := ","
		if i == len(fields)-1 {
			reste = ""
		}
		strBuilder.WriteString(fName + " " + fType + reste)
	}
	if len(fkeys) > 0 {
		strBuilder.WriteString(",")
	}
	for i, k := range fkeys {
		strBuilder.WriteString(k)
		if i < len(fkeys)-1 {
			strBuilder.WriteString(",")
		}
	}
	st := strBuilder.String()
	st = strings.TrimSuffix(st, ",")
	st += ");"
	return strings.ReplaceAll(st, ",,", ",")
}
