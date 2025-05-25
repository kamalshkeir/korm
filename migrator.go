package korm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/kamalshkeir/kstrct"
	"github.com/kamalshkeir/lg"
)

var triggersTables = make(map[string]struct{}, 0)

func GetTablesInfosFromDB(tables ...string) []TableEntity {
	if len(tables) == 0 {
		tables = GetAllTables(defaultDB)
	}
	tinfos, err := Model[TablesInfos]().Where("name IN (?)", tables).All()
	if lg.CheckError(err) {
		return nil
	}
	res := make([]TableEntity, 0, len(tinfos))
	for _, ti := range tinfos {
		fkk := []kormFkey{}
		for _, fk := range ti.Fkeys {
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
		res = append(res, TableEntity{
			Types:      ti.Types,
			ModelTypes: ti.ModelTypes,
			Tags:       ti.Tags,
			Columns:    ti.Columns,
			Pk:         ti.Pk,
			Name:       ti.Name,
			Fkeys:      fkk,
		})
	}
	return res
}

// CREATE TRIGGER IF NOT EXISTS users_update_trig AFTER UPDATE ON
func checkUpdatedAtTrigger(dialect, tableName, col, pk string) map[string][]string {
	triggers := map[string][]string{}
	t := "strftime('%s', 'now')"
	if dialect == SQLITE || dialect == "sqlite" {
		st := "CREATE TRIGGER IF NOT EXISTS "
		st += tableName + "_update_trig AFTER UPDATE ON " + tableName
		st += " BEGIN update " + tableName + " SET " + col + " = " + t
		st += " WHERE " + pk + " = " + "NEW." + pk + ";"
		st += "End;"
		triggers[col] = []string{st}
	} else if dialect == POSTGRES {
		st := "CREATE OR REPLACE FUNCTION updated_at_trig() RETURNS trigger AS $$"
		st += " BEGIN NEW." + col + " = extract(epoch from now());RETURN NEW;"
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

func autoMigrate[T any](model *T, db *DatabaseEntity, tableName string, execute bool) (string, error) {
	toReturnstats := []string{}
	dialect := db.Dialect
	s := reflect.ValueOf(model).Elem()
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
		if ftype.Kind() == reflect.Ptr {
			mFieldName_Type[fname] = ftype.Elem().String()
		} else {
			mFieldName_Type[fname] = ftype.String()
		}

		if ftag, ok := typeOfT.Field(i).Tag.Lookup("korm"); ok {
			tags := strings.Split(ftag, ";")
			for i, tag := range tags {
				if ftag == "-" {
					continue
				}
				tag := strings.TrimSpace(tag)
				if tag == "autoinc" || tag == "pk" || fname == "id" {
					pk = fname
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
		if v := strings.ToLower(typeOfT.Field(0).Name); strings.HasSuffix(v, "id") {
			pk = v
			mFieldName_Tags[pk] = []string{"pk"}
		} else {
			cols = append([]string{"id"}, cols...)
			mFieldName_Type["id"] = "uint"
			mFieldName_Tags["id"] = []string{"pk"}
			pk = "id"
		}
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
			if ty[0] != '[' && strings.Contains(ty, "int") {
				if ty[0] != '*' && ty[1] != '[' {
					handleMigrationInt(mi)
				}
			}
			switch ty {
			case "bool", "*bool":
				handleMigrationBool(mi)
			case "string", "*string", "[]string", "[]int", "[]int64", "[]float64", "[]any":
				handleMigrationString(mi)
			case "[]uint8", "[]byte", "*[]uint8", "*[]byte":
				handleMigrationSliceByte(mi)
			case "float64", "float32", "*float64", "*float32":
				handleMigrationFloat(mi)
			case "time.Time", "*time.Time":
				handleMigrationTime(mi)
			default:
				if strings.HasPrefix(ty, "[]") || strings.HasPrefix(ty, "*[]") {
					if tag, ok := (*mi.fTags)[mi.fName]; ok {
						tag = append(tag, "text")
					} else {
						(*mi.fTags)[mi.fName] = []string{"text"}
					}
					handleMigrationString(mi)
					continue
				}
				if strings.HasPrefix(ty, "map") || strings.HasPrefix(ty, "*map") {
					handleMigrationSliceByte(mi)
					continue
				}
				if tags, ok := mFieldName_Tags[fName]; ok {
					if strings.Contains(strings.Join(tags, ","), "json") {
						handleMigrationSliceByte(mi)
						continue
					}
					if !strings.Contains(strings.Join(tags, ","), "generated") && !strings.Contains(ty, "int") {
						lg.Errorf("%s of type %s not handled", fName, ty)
					}
				}
			}
		}
	}
	statement := prepareCreateStatement(tableName, res, fkeys, cols, db.Dialect)
	var triggers map[string][]string

	// check for update field to create a trigger
	if db.Dialect != MYSQL && db.Dialect != MARIA {
		for col, tags := range mFieldName_Tags {
			for _, tag := range tags {
				if tag == "update" {
					triggers = checkUpdatedAtTrigger(db.Dialect, tableName, col, pk)
				}
			}
		}
	} else {
		for col, tags := range mFieldName_Tags {
			for _, tag := range tags {
				if tag == "now" {
					createTrigger := fmt.Sprintf(`CREATE TRIGGER before_insert_%s_%s
					BEFORE INSERT ON %s
					FOR EACH ROW
					BEGIN
						SET NEW.%s = UNIX_TIMESTAMP();
					END`, tableName, col, tableName, col)

					if triggers == nil {
						triggers = make(map[string][]string)
					}
					triggers["mysql_triggers"] = append(triggers["mysql_triggers"], createTrigger)
				} else if tag == "update" {
					createTrigger := fmt.Sprintf(`CREATE TRIGGER before_update_%s_%s
					BEFORE UPDATE ON %s
					FOR EACH ROW
					BEGIN
						SET NEW.%s = UNIX_TIMESTAMP();
					END`, tableName, col, tableName, col)

					if triggers == nil {
						triggers = make(map[string][]string)
					}
					triggers["mysql_triggers"] = append(triggers["mysql_triggers"], createTrigger)
				}
			}
		}
	}

	if Debug {
		lg.InfoC("debug", "stat", statement)
	}

	c, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	if execute {
		ress, err := db.Conn.ExecContext(c, statement)
		if lg.CheckError(err) {
			lg.InfoC("debug", "stat", statement)
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
						lg.Printfs("trigger updated_at %s: %s\n", tableName, st)
					}
					if execute {
						err := Exec(db.Name, st)
						if lg.CheckError(err) {
							lg.Printfs("rdtrigger updated_at %s: %s\n", tableName, st)
							return "", err
						}
					}
					toReturnstats = append(toReturnstats, st)
				}
			}
		}
		statIndexes := ""
		if len(indexes) > 0 {
			for _, col := range indexes {
				ff := strings.ReplaceAll(col, "DESC", "")
				statIndexes += fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s);", tableName, ff, tableName, col)
			}
		}
		mstatIndexes := ""
		if len(*mi.mindexes) > 0 {
			for k, v := range *mi.mindexes {
				ff := strings.ReplaceAll(k, "DESC", "")
				mstatIndexes = fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s)", tableName, ff, tableName, k+","+v)
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
		statIndexesExecuted := false
		if statIndexes != "" {
			if Debug {
				lg.Printfs("%s\n", statIndexes)
			}
			if execute && !statIndexesExecuted {
				_, err := db.Conn.Exec(statIndexes)
				if lg.CheckError(err) {
					lg.Printfs("rdindexes: %s\n", statIndexes)
					return "", err
				}
				statIndexesExecuted = true
			}

			toReturnstats = append(toReturnstats, statIndexes)
		}
		if mstatIndexes != "" {
			if Debug {
				lg.Printfs("mindexes: %s\n", mstatIndexes)
			}
			if execute {
				_, err := db.Conn.Exec(mstatIndexes)
				if lg.CheckError(err) {
					lg.Printfs("rdmindexes: %s\n", mstatIndexes)
					return "", err
				}
			}

			toReturnstats = append(toReturnstats, mstatIndexes)
		}
		if len(ustatIndexes) > 0 {
			for i := range ustatIndexes {
				if Debug {
					lg.Printfs("uindexes: %s\n", ustatIndexes[i])
				}
				if execute {
					// Extract index name from the CREATE INDEX statement
					parts := strings.Split(ustatIndexes[i], " ")
					var indexName string
					for j, part := range parts {
						if part == "INDEX" {
							indexName = parts[j+1]
							break
						}
					}
					// Check if index exists before creating it
					if !indexExists(db.Conn, tableName, indexName, db.Dialect) {
						_, err := db.Conn.Exec(ustatIndexes[i])
						if lg.CheckError(err) {
							lg.Printfs("rduindexes: %s\n", ustatIndexes)
							return "", err
						}
					}
				}
				toReturnstats = append(toReturnstats, ustatIndexes[i])
			}
		}
	}
	if execute && Debug {
		lg.Printfs("gr %s migrated\n", tableName)
	}
	toReturnQuery := strings.Join(toReturnstats, ";")
	return toReturnQuery, nil
}

func autoMigrateAny(model any, db *DatabaseEntity, tableName string, execute bool) (string, error) {
	toReturnstats := []string{}
	dialect := db.Dialect
	s := reflect.ValueOf(model)
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
		if ftype.Kind() == reflect.Ptr {
			mFieldName_Type[fname] = ftype.Elem().String()
		} else {
			mFieldName_Type[fname] = ftype.String()
		}

		if ftag, ok := typeOfT.Field(i).Tag.Lookup("korm"); ok {
			tags := strings.Split(ftag, ";")
			for i, tag := range tags {
				if ftag == "-" {
					continue
				}
				tag := strings.TrimSpace(tag)
				if tag == "autoinc" || tag == "pk" || fname == "id" {
					pk = fname
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
			if ty[0] != '[' && strings.Contains(ty, "int") {
				if ty[0] != '*' && ty[1] != '[' {
					handleMigrationInt(mi)
				}
			}
			switch ty {
			case "bool", "*bool":
				handleMigrationBool(mi)
			case "string", "*string", "[]string", "[]int", "[]int64", "[]float64", "[]any":
				handleMigrationString(mi)
			case "[]uint8", "[]byte", "*[]uint8", "*[]byte":
				handleMigrationSliceByte(mi)
			case "float64", "float32", "*float64", "*float32":
				handleMigrationFloat(mi)
			case "time.Time", "*time.Time":
				handleMigrationTime(mi)
			default:
				if tags, ok := mFieldName_Tags[fName]; ok {
					if !strings.Contains(strings.Join(tags, ","), "generated") && !strings.Contains(ty, "int") {
						lg.Errorf("%s of type %s not handled", fName, ty)
					}
				}
			}
		}
	}
	statement := prepareCreateStatement(tableName, res, fkeys, cols, db.Dialect)
	var triggers map[string][]string

	// check for update field to create a trigger
	if db.Dialect != MYSQL && db.Dialect != MARIA {
		for col, tags := range mFieldName_Tags {
			for _, tag := range tags {
				if tag == "update" {
					triggers = checkUpdatedAtTrigger(db.Dialect, tableName, col, pk)
				}
			}
		}
	} else {
		for col, tags := range mFieldName_Tags {
			for _, tag := range tags {
				if tag == "now" {
					createTrigger := fmt.Sprintf(`CREATE TRIGGER before_insert_%s_%s
					BEFORE INSERT ON %s
					FOR EACH ROW
					BEGIN
						SET NEW.%s = UNIX_TIMESTAMP();
					END`, tableName, col, tableName, col)

					if triggers == nil {
						triggers = make(map[string][]string)
					}
					triggers["mysql_triggers"] = append(triggers["mysql_triggers"], createTrigger)
				} else if tag == "update" {
					createTrigger := fmt.Sprintf(`CREATE TRIGGER before_update_%s_%s
					BEFORE UPDATE ON %s
					FOR EACH ROW
					BEGIN
						SET NEW.%s = UNIX_TIMESTAMP();
					END`, tableName, col, tableName, col)

					if triggers == nil {
						triggers = make(map[string][]string)
					}
					triggers["mysql_triggers"] = append(triggers["mysql_triggers"], createTrigger)
				}
			}
		}
	}

	if Debug {
		lg.InfoC("debug", "stat", statement)
	}

	c, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	if execute {
		ress, err := db.Conn.ExecContext(c, statement)
		if lg.CheckError(err) {
			lg.InfoC("debug", "stat", statement)
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
						lg.Printfs("trigger updated_at %s: %s\n", tableName, st)
					}
					if execute {
						err := Exec(db.Name, st)
						if lg.CheckError(err) {
							lg.Printfs("rdtrigger updated_at %s: %s\n", tableName, st)
							return "", err
						}
					}
					toReturnstats = append(toReturnstats, st)
				}
			}
		}
		statIndexes := ""
		if len(indexes) > 0 {
			for _, col := range indexes {
				ff := strings.ReplaceAll(col, "DESC", "")
				statIndexes += fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s);", tableName, ff, tableName, col)
			}
		}
		mstatIndexes := ""
		if len(*mi.mindexes) > 0 {
			for k, v := range *mi.mindexes {
				ff := strings.ReplaceAll(k, "DESC", "")
				mstatIndexes = fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s)", tableName, ff, tableName, k+","+v)
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
		statIndexesExecuted := false
		if statIndexes != "" {
			if Debug {
				lg.Printfs("%s\n", statIndexes)
			}
			if execute && !statIndexesExecuted {
				_, err := db.Conn.Exec(statIndexes)
				if lg.CheckError(err) {
					lg.Printfs("rdindexes: %s\n", statIndexes)
					return "", err
				}
				statIndexesExecuted = true
			}

			toReturnstats = append(toReturnstats, statIndexes)
		}
		if mstatIndexes != "" {
			if Debug {
				lg.Printfs("mindexes: %s\n", mstatIndexes)
			}
			if execute {
				_, err := db.Conn.Exec(mstatIndexes)
				if lg.CheckError(err) {
					lg.Printfs("rdmindexes: %s\n", mstatIndexes)
					return "", err
				}
			}

			toReturnstats = append(toReturnstats, mstatIndexes)
		}
		if len(ustatIndexes) > 0 {
			for i := range ustatIndexes {
				if Debug {
					lg.Printfs("uindexes: %s\n", ustatIndexes[i])
				}
				if execute {
					// Extract index name from the CREATE INDEX statement
					parts := strings.Split(ustatIndexes[i], " ")
					var indexName string
					for j, part := range parts {
						if part == "INDEX" {
							indexName = parts[j+1]
							break
						}
					}
					// Check if index exists before creating it
					if !indexExists(db.Conn, tableName, indexName, db.Dialect) {
						_, err := db.Conn.Exec(ustatIndexes[i])
						if lg.CheckError(err) {
							lg.Printfs("rduindexes: %s\n", ustatIndexes)
							return "", err
						}
					}
				}
				toReturnstats = append(toReturnstats, ustatIndexes[i])
			}
		}
	}
	if execute && Debug {
		lg.Printfs("gr %s migrated\n", tableName)
	}
	toReturnQuery := strings.Join(toReturnstats, ";")
	return toReturnQuery, nil
}

func AutoMigrate[T any](tableName string, dbName ...string) error {
	mutexModelTablename.Lock()
	foundm := false
	for k := range mModelTablename {
		if k == tableName {
			foundm = true
		}
	}
	if !foundm {
		mModelTablename[tableName] = *new(T)
	}
	mutexModelTablename.Unlock()
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
	if !tbFoundDB {
		_, err := autoMigrate(new(T), db, tableName, true)
		if lg.CheckError(err) {
			return err
		}
	}
	LinkModel[T](tableName, db.Name)
	if tableName != "users" && tableName != "_triggers_queue" && tableName != "_tables_infos" {
		if _, ok := triggersTables[tableName]; !ok {
			err = AddChangesTrigger(tableName)
			if !lg.CheckError(err) {
				triggersTables[tableName] = struct{}{}
			}
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
	genas, primary, autoinc, notnull, defaultt, checks, unique := "", "", "", "", "", []string{}, ""
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
					lg.ErrorC("not supported dialect")
				}
			case "notnull":
				notnull = " NOT NULL"
			case "index", "+index", "index+":
				*mi.indexes = append(*mi.indexes, mi.fName)
			case "-index", "index-":
				*mi.indexes = append(*mi.indexes, mi.fName+" DESC")
			case "unique":
				unique = " UNIQUE"
			case "default":
				defaultt = " DEFAULT 0"
			default:
				lg.ErrorC("tag not handled", "tag", tag)
			}
		} else {
			// with params
			sp := strings.Split(tag, ":")
			tg := sp[0]
			switch tg {
			case "default":
				defaultt = " DEFAULT " + sp[1]
			case "generated":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
				genas = " GENERATED ALWAYS AS (" + sp[1] + ") STORED "
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
							lg.ErrorC("fk not handled", "fk", sp[2])
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
								lg.ErrorC("fk not handled", "fk", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					lg.ErrorC("allowed options cascade/donothing/noaction")
				}
			case "check":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
				checks = append(checks, strings.TrimSpace(sp[1]))
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						lg.ErrorC("mindex not working for", "fname", mi.fName, "sp", sp[1])
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
						lg.ErrorC("mindex not working for", "fname", mi.fName, "sp", sp[1])
					}
				} else {
					(*mi.uindexes)[mi.fName] = sp[1]
				}
			default:
				lg.ErrorC("MIGRATION INT: not handled for field", "fname", mi.fName, "v", sp[0], "tag", tag)
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
		if defaultt != "" {
			if genas == "" {
				(*mi.res)[mi.fName] += defaultt
			} else {
				(*mi.res)[mi.fName] += genas
			}
		} else if genas != "" {
			(*mi.res)[mi.fName] += genas
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
		(*mi.res)[mi.fName] = "INTEGER DEFAULT 0 CHECK (" + mi.fName + " IN (0, 1))"
		return
	}
	for _, tag := range tags {
		if strings.Contains(tag, ":") {
			sp := strings.Split(tag, ":")
			switch sp[0] {
			case "default":
				if sp[1] != "" {
					if strings.Contains(sp[1], "true") {
						defaultt = " DEFAULT 1"
					} else if strings.Contains(sp[1], "false") {
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
						lg.ErrorC("mindex not working for", "field", mi.fName, "v", sp[1])
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
							lg.ErrorC("fk not handled action", "ac", sp[2])
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
								lg.ErrorC("fk not handled action", "ac", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					lg.ErrorC("fk should be fk:users.id:cascade/donothing")
				}
			default:
				lg.ErrorC("not handled migration bool", "v", sp[0], "field", mi.fName)
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
				lg.ErrorC("not handled in Migration Bool", "tag", tag)
			}
		}
	}
	if defaultt != "" {
		(*mi.res)[mi.fName] = "INTEGER" + defaultt + " CHECK (" + mi.fName + " IN (0, 1))"
	}
	if (*mi.res)[mi.fName] == "" {
		(*mi.res)[mi.fName] = "INTEGER DEFAULT 0 CHECK (" + mi.fName + " IN (0, 1))"
	}
}

func handleMigrationString(mi *migrationInput) {
	unique, notnull, text, json, defaultt, genas, size, checks := "", "", "", "", "", "", "", []string{}
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
			case "json":
				json = "TEXT"
				if mi.dialect != SQLITE {
					json = "JSONB"
				}
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
				lg.ErrorC("not handled migration String", "tag", tag)
			}
		} else {
			sp := strings.Split(tag, ":")
			switch sp[0] {
			case "default":
				defaultt = " DEFAULT " + sp[1]
			case "generated":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
				genas = " GENERATED ALWAYS AS (" + sp[1] + ") STORED "
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						lg.ErrorC("mindex not working", "f", mi.fName, "v", sp[1])
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
						lg.ErrorC("mindex not working", "f", mi.fName, "v", sp[1])
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
							lg.ErrorC("fk not handled", "fk", sp[2])
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
								lg.ErrorC("fk not handled", "fk", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					lg.ErrorC("foreign key should be like fk:table.column:[cascade/donothing]:[cascade/donothing]")
				}
			case "size":
				sp := strings.Split(tag, ":")
				if sp[0] == "size" {
					size = sp[1]
				}
			case "check":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
				checks = append(checks, strings.TrimSpace(sp[1]))
			default:
				lg.ErrorC("migration String not handled for", "v", sp[0], "tag", tag, "f", mi.fName)
			}
		}
	}

	if text != "" {
		(*mi.res)[mi.fName] = text
	} else if json != "" {
		(*mi.res)[mi.fName] = json
	} else {
		if size != "" {
			(*mi.res)[mi.fName] = "VARCHAR(" + size + ")"
		} else {
			(*mi.res)[mi.fName] = "VARCHAR(255)"
		}
	}

	if notnull != "" {
		(*mi.res)[mi.fName] += notnull
	}
	if unique != "" {
		(*mi.res)[mi.fName] += unique
	}
	if defaultt != "" {
		if genas == "" {
			(*mi.res)[mi.fName] += defaultt
		} else {
			(*mi.res)[mi.fName] += genas
		}
	} else if genas != "" {
		(*mi.res)[mi.fName] += genas
	}
	if len(checks) > 0 {
		joined := strings.TrimSpace(strings.Join(checks, " AND "))
		(*mi.res)[mi.fName] += " CHECK(" + joined + ")"
	}
}

func handleMigrationSliceByte(mi *migrationInput) {
	unique, notnull, defaultt, genas, size, json, pk, checks := "", "", "", "", "", "", "", []string{}
	tags := (*mi.fTags)[mi.fName]
	if len(tags) == 1 && tags[0] == "-" {
		(*mi.res)[mi.fName] = ""
		return
	}
	for _, tag := range tags {
		if !strings.Contains(tag, ":") {
			switch tag {
			case "text":
				json = "TEXT"
			case "json":
				json = "TEXT"
				if mi.dialect != SQLITE {
					json = "JSONB"
				}
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
				lg.ErrorC("tag not handled for migration string", "tag", tag)
			}
		} else {
			sp := strings.Split(tag, ":")
			switch sp[0] {
			case "default":
				defaultt = " DEFAULT " + sp[1]
			case "generated":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
				genas = " GENERATED ALWAYS AS (" + sp[1] + ") STORED "
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						lg.ErrorC("mindex not working", "f", mi.fName, "v", sp[1])
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
						lg.ErrorC("mindex not working", "f", mi.fName, "v", sp[1])
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
							lg.ErrorC("fk not handled", "v", sp[2])
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
								lg.ErrorC("fk not handled", "v", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					lg.ErrorC("foreign key should be like fk:table.column:[cascade/donothing]:[cascade/donothing]")
				}
			case "size":
				sp := strings.Split(tag, ":")
				if sp[0] == "size" {
					size = sp[1]
				}
			case "check":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
				checks = append(checks, strings.TrimSpace(sp[1]))
			default:
				lg.ErrorC("Migration string not handled", "v", sp[0], "tag", tag, "f", mi.fName)
			}
		}
	}

	if json == "" {
		if size == "" {
			if mi.dialect == "postgres" || mi.dialect == "pg" {
				(*mi.res)[mi.fName] = "BYTEA"
			} else {
				(*mi.res)[mi.fName] = "BLOB"
			}
		} else {
			switch mi.dialect {
			case "mysql":
				(*mi.res)[mi.fName] = "VARBINARY(" + size + ")"
			case "postgres", "pg":
				(*mi.res)[mi.fName] = "BIT VARYING(" + size + ")"
			default:
				(*mi.res)[mi.fName] = "BLOB"
			}
		}
	} else {
		(*mi.res)[mi.fName] = json
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
		if genas == "" {
			(*mi.res)[mi.fName] += defaultt
		} else {
			(*mi.res)[mi.fName] += genas
		}
	} else if genas != "" {
		(*mi.res)[mi.fName] += genas
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
				lg.ErrorC("tag not handled for migration float", "tag", tag)
			}
		} else {
			sp := strings.Split(tag, ":")
			switch sp[0] {
			case "default":
				if sp[1] != "" {
					mtags["default"] = " DEFAULT " + sp[1]
				}
			case "generated":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
				mtags["default"] = " GENERATED ALWAYS AS (" + sp[1] + ") STORED "
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
							lg.ErrorC("fk not handled action", "action", sp[2])
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
								lg.ErrorC("fk not handled", "action", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					lg.ErrorC("foreign key should be like fk:table.column:[cascade/donothing]")
				}
			case "mindex":
				if v, ok := (*mi.mindexes)[mi.fName]; ok {
					if v == "" {
						(*mi.mindexes)[mi.fName] = sp[1]
					} else if strings.Contains(sp[1], ",") {
						(*mi.mindexes)[mi.fName] += "," + sp[1]
					} else {
						lg.ErrorC("mindex not working for", "f", mi.fName, "v", sp[1])
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
						lg.ErrorC("mindex not working", "f", mi.fName, "v", sp[1])
					}
				} else {
					(*mi.uindexes)[mi.fName] = sp[1]
				}
			case "check":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
				if v, ok := mtags["check"]; ok && v != "" {
					mtags["check"] += " AND " + strings.TrimSpace(sp[1])
				} else if v == "" {
					mtags["check"] = strings.TrimSpace(sp[1])
				}
			default:
				lg.ErrorC("MIGRATION FLOAT: not handled", "v", sp[0], "tag", tag, "f", mi.fName)
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
				lg.ErrorC("case not handled", "case", k)
			}
		}
	}
}

func handleMigrationTime(mi *migrationInput) {
	defaultt, notnull, check, unique := "", "", "", ""
	tags := (*mi.fTags)[mi.fName]
	if len(tags) == 1 && tags[0] == "-" {
		(*mi.res)[mi.fName] = ""
		return
	}
	for _, tag := range tags {
		if !strings.Contains(tag, ":") {
			switch tag {
			case "unique":
				unique = " UNIQUE"
			case "now":
				switch mi.dialect {
				case SQLITE, "":
					defaultt = "BIGINT NOT NULL DEFAULT (strftime('%s', 'now'))"
				case POSTGRES:
					defaultt = "BIGINT NOT NULL DEFAULT extract(epoch from now())"
				case MYSQL, MARIA:
					defaultt = "BIGINT NOT NULL DEFAULT 0"
				default:
					lg.ErrorC("not handled Time for", "f", mi.fName, "type", mi.fType)
				}
			case "update":
				switch mi.dialect {
				case SQLITE, "":
					defaultt = "BIGINT NOT NULL DEFAULT (strftime('%s', 'now'))"
				case POSTGRES:
					defaultt = "BIGINT NOT NULL DEFAULT extract(epoch from now())"
				case MYSQL, MARIA:
					defaultt = "BIGINT NOT NULL DEFAULT 0"
				default:
					lg.ErrorC("not handled Time for", "f", mi.fName, "type", mi.fType)
				}
			case "index", "+index", "index+":
				*mi.indexes = append(*mi.indexes, mi.fName)
			case "-index", "index-":
				*mi.indexes = append(*mi.indexes, mi.fName+" DESC")
			default:
				lg.ErrorC("time tag not handled", "tag", tag)
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
							lg.ErrorC("fk action not handled", "action", sp[2])
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
								lg.ErrorC("fk action not handled", "action", sp[3])
							}
						}
					}
					*mi.fKeys = append(*mi.fKeys, fkey)
				} else {
					lg.ErrorC("it should be fk:users.id:cascade/donothing")
				}
			case "check":
				sp[1] = adaptConcatAndLen(sp[1], mi.dialect)
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
						lg.ErrorC("mindex not working", "f", mi.fName, "v", sp[1])
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
						lg.ErrorC("mindex not working", "f", mi.fName, "v", sp[1])
					}
				} else {
					(*mi.uindexes)[mi.fName] = sp[1]
				}
			default:
				lg.ErrorC("case not handled for time", "case", sp[0])
			}
		}
	}
	if defaultt != "" {
		if unique != "" {
			(*mi.res)[mi.fName] += unique
		}
		(*mi.res)[mi.fName] = defaultt
	} else {
		(*mi.res)[mi.fName] = "BIGINT"
		if notnull != "" {
			(*mi.res)[mi.fName] += notnull
		}
		if unique != "" {
			(*mi.res)[mi.fName] += unique
		}
		if check != "" {
			(*mi.res)[mi.fName] += " CHECK(" + check + ")"
		}
	}
}

func prepareCreateStatement(tbName string, fields map[string]string, fkeys, cols []string, dialect string) string {
	var strBuilder strings.Builder
	if dialect == POSTGRES || dialect == COCKROACH {
		strBuilder.WriteString(`CREATE TABLE IF NOT EXISTS "` + tbName + `" (`)
		for i, col := range cols {
			fName := `"` + col + `"`
			fType := fields[col]
			if fType == "" {
				continue
			}
			reste := ","
			if i == len(cols)-1 {
				reste = ""
			}
			strBuilder.WriteString(fName + " " + fType + reste)
		}
	} else {
		strBuilder.WriteString("CREATE TABLE IF NOT EXISTS `" + tbName + "` (")
		for i, col := range cols {
			fName := "`" + col + "`"
			fType := fields[col]
			if fType == "" {
				continue
			}
			reste := ","
			if i == len(cols)-1 {
				reste = ""
			}
			strBuilder.WriteString(fName + " " + fType + reste)
		}
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
