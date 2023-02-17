package korm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/kstrct"
)

var (
	cacheOneS = kmap.New[dbCache, any](false)
	cacheAllS = kmap.New[dbCache, any](false, cacheMaxMemoryMb)
)

// BuilderS is query builder for struct using generics
type BuilderS[T comparable] struct {
	debug      bool
	limit      int
	page       int
	tableName  string
	selected   string
	orderBys   string
	whereQuery string
	offset     string
	statement  string
	database   string
	args       []any
	order      []string
	ctx        context.Context
}

// Model is a starter for Buider
func Model[T comparable](model ...T) *BuilderS[T] {
	tName := getTableName[T]()
	if tName == "" {
		return nil
	}
	return &BuilderS[T]{
		tableName: tName,
		database:  databases[0].Name,
	}
}

func ModelTable[T comparable](tableName string, model ...T) *BuilderS[T] {
	tName := getTableName[T]()
	if tName != tableName {
		mutexModelTablename.Lock()
		mModelTablename[*new(T)] = tableName
		mutexModelTablename.Unlock()
		tName = tableName
	}
	return &BuilderS[T]{
		tableName: tName,
		database:  databases[0].Name,
	}
}

// Database allow to choose database to execute query on
func (b *BuilderS[T]) Database(dbName string, model ...T) *BuilderS[T] {
	if b == nil || b.tableName == "" {
		return nil
	}
	b.database = dbName
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
		b.database = databases[0].Name
	} else {
		b.database = db.Name
	}
	return b
}

// Insert insert a row into a table and return inserted PK
func (b *BuilderS[T]) Insert(model *T) (int, error) {
	if b == nil || b.tableName == "" {
		return 0, ErrTableNotFound
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "create",
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}

	names, mvalues, mTypes, mtags := getStructInfos(model, true)
	values := []any{}
	if len(names) < len(mvalues) {
		return 0, errors.New("more values than fields")
	}
	if onInsert != nil {
		err := onInsert(b.database, b.tableName, mvalues)
		if err != nil {
			return 0, err
		}
	}
	placeholdersSlice := []string{}
	ignored := []int{}
	pk := ""
	for i, name := range names {
		if v, ok := mvalues[name]; ok {
			if v == true {
				v = 1
			} else if v == false {
				v = 0
			}
			if vvv, ok := mTypes[name]; ok && strings.HasSuffix(vvv, "Time") {
				switch tyV := v.(type) {
				case time.Time:
					v = tyV.Unix()
				case string:
					v = strings.ReplaceAll(tyV, "T", " ")
				}
			}
			values = append(values, v)
		} else {
			klog.Printf("rd%vnot found in fields\n")
			return 0, errors.New("field not found")
		}

		if tags, ok := mtags[name]; ok {
			ig := false
			for _, tag := range tags {
				switch tag {
				case "autoinc", "pk", "-":
					pk = name
					ig = true
				default:
					if strings.Contains(tag, "generated") {
						ig = true
					}
				}
			}
			if ig {
				ignored = append(ignored, i)
			} else {
				placeholdersSlice = append(placeholdersSlice, "?")
			}
		} else {
			placeholdersSlice = append(placeholdersSlice, "?")
		}
	}
	cum := 0
	for _, ign := range ignored {
		ii := ign - cum
		delete(mvalues, names[ii])
		names = append(names[:ii], names[ii+1:]...)
		values = append(values[:ii], values[ii+1:]...)
		cum++
	}
	placeholders := strings.Join(placeholdersSlice, ",")
	fields_comma_separated := strings.Join(names, ",")

	stat := strings.Builder{}
	stat.WriteString("INSERT INTO " + b.tableName + " (")
	stat.WriteString(fields_comma_separated)
	stat.WriteString(") VALUES (")
	stat.WriteString(placeholders)
	stat.WriteString(")")
	b.statement = stat.String()
	adaptPlaceholdersToDialect(&b.statement, db.Dialect)

	if db.Dialect != POSTGRES {
		var res sql.Result
		if b.debug {
			klog.Printf("statement : %s, values : %s\n", b.statement, values)
		}
		if b.ctx != nil {
			res, err = db.Conn.ExecContext(b.ctx, b.statement, values...)
		} else {
			res, err = db.Conn.Exec(b.statement, values...)
		}
		if err != nil {
			return 0, err
		}
		rows, err := res.LastInsertId()
		if err != nil {
			return int(rows), err
		}
		return int(rows), nil
	} else {
		var id int
		if b.debug {
			klog.Printf("statement : %s, values : %s\n", b.statement+" RETURNING "+pk, values)
		}
		if b.ctx != nil {
			err = db.Conn.QueryRowContext(b.ctx, b.statement+" RETURNING "+pk, values...).Scan(&id)
		} else {
			err = db.Conn.QueryRow(b.statement+" RETURNING "+pk, values...).Scan(&id)
		}
		if err != nil {
			return id, err
		}
		return id, nil
	}
}

// InsertR add row to a table using input struct, and return the inserted row
func (b *BuilderS[T]) InsertR(model *T) (T, error) {
	if b == nil || b.tableName == "" {
		return *new(T), ErrTableNotFound
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "create",
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return *new(T), err
	}

	names, mvalues, mTypes, mtags := getStructInfos(model, true)
	values := []any{}
	if len(names) < len(mvalues) {
		return *new(T), errors.New("more values than fields")
	}
	if onInsert != nil {
		err := onInsert(b.database, b.tableName, mvalues)
		if err != nil {
			return *new(T), err
		}
	}
	placeholdersSlice := []string{}
	ignored := []int{}
	pk := ""
	for i, name := range names {
		if v, ok := mvalues[name]; ok {
			if v == true {
				v = 1
			} else if v == false {
				v = 0
			}
			if vvv, ok := mTypes[name]; ok && strings.HasSuffix(vvv, "Time") {
				switch tyV := v.(type) {
				case time.Time:
					v = tyV.Unix()
				case string:
					v = strings.ReplaceAll(tyV, "T", " ")
				}
			}
			values = append(values, v)
		} else {
			klog.Printf("rd%vnot found in fields\n")
			return *new(T), errors.New("field not found")
		}

		if tags, ok := mtags[name]; ok {
			ig := false
			for _, tag := range tags {
				switch tag {
				case "autoinc", "pk", "-":
					pk = name
					ig = true
				default:
					if strings.Contains(tag, "generated") {
						ig = true
					}
				}
			}
			if ig {
				ignored = append(ignored, i)
			} else {
				placeholdersSlice = append(placeholdersSlice, "?")
			}
		} else {
			placeholdersSlice = append(placeholdersSlice, "?")
		}
	}

	cum := 0
	for _, ign := range ignored {
		ii := ign - cum
		delete(mvalues, names[ii])
		names = append(names[:ii], names[ii+1:]...)
		values = append(values[:ii], values[ii+1:]...)
		cum++
	}
	placeholders := strings.Join(placeholdersSlice, ",")
	fields_comma_separated := strings.Join(names, ",")

	stat := strings.Builder{}
	stat.WriteString("INSERT INTO " + b.tableName + " (")
	stat.WriteString(fields_comma_separated)
	stat.WriteString(") VALUES (")
	stat.WriteString(placeholders)
	stat.WriteString(")")
	b.statement = stat.String()
	adaptPlaceholdersToDialect(&b.statement, db.Dialect)
	if b.debug {
		klog.Printf("statement : %s, values : %s\n", b.statement, values)
	}
	var id int
	if db.Dialect != POSTGRES {
		var res sql.Result
		if b.ctx != nil {
			res, err = db.Conn.ExecContext(b.ctx, b.statement, values...)
		} else {
			res, err = db.Conn.Exec(b.statement, values...)
		}
		if err != nil {
			return *new(T), err
		}
		rows, err := res.LastInsertId()
		if err != nil {
			return *new(T), err
		}
		id = int(rows)
	} else {
		if b.ctx != nil {
			err = db.Conn.QueryRowContext(b.ctx, b.statement+" RETURNING "+pk, values...).Scan(&id)
		} else {
			err = db.Conn.QueryRow(b.statement+" RETURNING "+pk, values...).Scan(&id)
		}
		if err != nil {
			return *new(T), err
		}
	}
	m, err := Model[T]().Where(pk+"=?", id).One()
	if err != nil {
		return *new(T), err
	}
	return m, nil
}

// BulkInsert insert many row at the same time in one query
func (b *BuilderS[T]) BulkInsert(models ...*T) ([]int, error) {
	if b == nil || b.tableName == "" {
		return nil, ErrTableNotFound
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "create",
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
		return nil, err
	}
	tx, err := db.Conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	ids := []int{}
	pk := ""
	for mm := range models {
		names, mvalues, mTypes, mtags := getStructInfos(models[mm], true)
		if len(names) < len(mvalues) {
			return nil, errors.New("more values than fields")
		}
		if onInsert != nil {
			err := onInsert(b.database, b.tableName, mvalues)
			if err != nil {
				return nil, err
			}
		}
		placeholdersSlice := []string{}
		values := []any{}
		ignored := []int{}
		for i, name := range names {
			if v, ok := mvalues[name]; ok {
				if v == true {
					v = 1
				} else if v == false {
					v = 0
				}
				if fType, ok := mTypes[name]; ok && strings.HasSuffix(fType, "Time") {
					switch tyV := v.(type) {
					case time.Time:
						v = tyV.Unix()
					case string:
						v = strings.ReplaceAll(tyV, "T", " ")
					}
				}
				values = append(values, v)
			} else {
				return nil, fmt.Errorf("field value not found")
			}

			if tags, ok := mtags[name]; ok {
				ig := false
				for _, tag := range tags {
					switch tag {
					case "autoinc", "pk", "-":
						ig = true
						pk = name
					default:
						if strings.Contains(tag, "generated") {
							ig = true
						}
					}
				}
				if ig {
					ignored = append(ignored, i)
				} else {
					placeholdersSlice = append(placeholdersSlice, "?")
				}
			} else {
				placeholdersSlice = append(placeholdersSlice, "?")
			}
		}
		cum := 0
		for _, ign := range ignored {
			ii := ign - cum
			delete(mvalues, names[ii])
			names = append(names[:ii], names[ii+1:]...)
			values = append(values[:ii], values[ii+1:]...)
			cum++
		}
		ph := strings.Join(placeholdersSlice, ",")
		fields_comma_separated := strings.Join(names, ",")
		stat := strings.Builder{}
		stat.WriteString("INSERT INTO " + b.tableName + " (")
		stat.WriteString(fields_comma_separated)
		stat.WriteString(") VALUES (")
		stat.WriteString(ph)
		stat.WriteString(");")
		statem := stat.String()
		adaptPlaceholdersToDialect(&statem, db.Dialect)
		if b.debug {
			klog.Printf("%s,%s\n", statem, values)
		}

		if db.Dialect != POSTGRES {
			var res sql.Result
			if b.ctx != nil {
				res, err = db.Conn.ExecContext(b.ctx, statem, values...)
			} else {
				res, err = db.Conn.Exec(statem, values...)
			}
			if err != nil {
				errRoll := tx.Rollback()
				if errRoll != nil {
					return nil, errRoll
				}
				return nil, err
			}
			idInserted, err := res.LastInsertId()
			if err != nil {
				return ids, err
			}
			ids = append(ids, int(idInserted))
		} else {
			var idInserted int
			if b.ctx != nil {
				err = db.Conn.QueryRowContext(b.ctx, statem+" RETURNING "+pk, values...).Scan(&idInserted)
			} else {
				err = db.Conn.QueryRow(statem+" RETURNING "+pk, values...).Scan(&idInserted)
			}
			if err != nil {
				return ids, err
			}
			ids = append(ids, idInserted)
		}
	}
	err = tx.Commit()
	if err != nil {
		return ids, err
	}
	return ids, nil
}

// AddRelated used for many to many, and after korm.ManyToMany, to add a class to a student or a student to a class, class or student should exist in the database before adding them
func (b *BuilderS[T]) AddRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error) {
	if b == nil || b.tableName == "" {
		return 0, ErrTableNotFound
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "create",
		})
	}

	db, _ := GetMemoryDatabase(b.database)

	relationTableName := "m2m_" + b.tableName + "-" + b.database + "-" + relatedTable
	if _, ok := relationsMap.Get("m2m_" + b.tableName + "-" + b.database + "-" + relatedTable); !ok {
		relationTableName = "m2m_" + relatedTable + "-" + b.database + "-" + b.tableName
		if _, ok2 := relationsMap.Get("m2m_" + relatedTable + "-" + b.database + "-" + b.tableName); !ok2 {
			return 0, fmt.Errorf("no relations many to many between theses 2 tables: %s, %s", b.tableName, relatedTable)
		}
	}
	cols := ""
	wherecols := ""
	inOrder := false
	if strings.HasPrefix(relationTableName, "m2m_"+b.tableName) {
		inOrder = true
		relationTableName = "m2m_" + b.tableName + "_" + relatedTable
		cols = b.tableName + "_id," + relatedTable + "_id"
		wherecols = b.tableName + "_id = ? and " + relatedTable + "_id = ?"
	} else if strings.HasPrefix(relationTableName, "m2m_"+relatedTable) {
		relationTableName = "m2m_" + relatedTable + "_" + b.tableName
		cols = relatedTable + "_id," + b.tableName + "_id"
		wherecols = relatedTable + "_id = ? and " + b.tableName + "_id = ?"
	}
	memoryRelatedTable, err := GetMemoryTable(relatedTable)
	if err != nil {
		return 0, fmt.Errorf("memory table not found:" + relatedTable)
	}
	memoryTypedTable, err := GetMemoryTable(b.tableName)
	if err != nil {
		return 0, fmt.Errorf("memory table not found:" + relatedTable)
	}

	adaptTrueFalseArgs(&whereRelatedArgs)
	whereRelatedTable = adaptConcatAndLen(whereRelatedTable, db.Dialect)
	adaptWhereQuery(&whereRelatedTable, relatedTable)
	data, err := Table(relatedTable).Where(whereRelatedTable, whereRelatedArgs...).One()
	if err != nil {
		return 0, err
	}
	ids := make([]any, 4)
	if v, ok := data[memoryRelatedTable.Pk]; ok {
		if inOrder {
			ids[1] = v
			ids[3] = v
		} else {
			ids[0] = v
			ids[2] = v
		}
	}
	// get the typed model
	if b.whereQuery == "" {
		return 0, fmt.Errorf("you must specify a where for the typed struct")
	}
	typedModel, err := Table(b.tableName).Where(b.whereQuery, b.args...).One()
	if err != nil {
		return 0, err
	}
	if v, ok := typedModel[memoryTypedTable.Pk]; ok {
		if inOrder {
			ids[0] = v
			ids[2] = v
		} else {
			ids[1] = v
			ids[3] = v
		}
	}
	if onInsert != nil {
		var mInsert map[string]any
		if inOrder {
			mInsert = map[string]any{
				b.tableName + "_id":  ids[0],
				relatedTable + "_id": ids[1],
			}
		} else {
			mInsert = map[string]any{
				b.tableName + "_id":  ids[1],
				relatedTable + "_id": ids[0],
			}
		}
		err := onInsert(db.Name, relationTableName, mInsert)
		if err != nil {
			return 0, err
		}
	}
	stat := "INSERT INTO " + relationTableName + "(" + cols + ") SELECT ?,? WHERE NOT EXISTS (SELECT * FROM " + relationTableName + " WHERE " + wherecols + ");"
	adaptPlaceholdersToDialect(&stat, db.Dialect)
	if b.debug {
		klog.Printf("statement:%s\n", stat)
		klog.Printf("args:%v\n", ids)
	}
	err = Exec(b.database, stat, ids...)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// DeleteRelated delete a relations many to many
func (b *BuilderS[T]) DeleteRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error) {
	if b == nil || b.tableName == "" {
		return 0, ErrTableNotFound
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "delete",
		})
	}
	relationTableName := "m2m_" + b.tableName + "-" + b.database + "-" + relatedTable
	if _, ok := relationsMap.Get("m2m_" + b.tableName + "-" + b.database + "-" + relatedTable); !ok {
		relationTableName = "m2m_" + relatedTable + "-" + b.database + "-" + b.tableName
		if _, ok2 := relationsMap.Get("m2m_" + relatedTable + "-" + b.database + "-" + b.tableName); !ok2 {
			return 0, fmt.Errorf("no relations many to many between theses 2 tables: %s, %s", b.tableName, relatedTable)
		}
	}

	wherecols := ""
	inOrder := false
	if strings.HasPrefix(relationTableName, "m2m_"+b.tableName) {
		inOrder = true
		relationTableName = "m2m_" + b.tableName + "_" + relatedTable
		wherecols = b.tableName + "_id = ? and " + relatedTable + "_id = ?"
	} else if strings.HasPrefix(relationTableName, "m2m_"+relatedTable) {
		relationTableName = "m2m_" + relatedTable + "_" + b.tableName
		wherecols = relatedTable + "_id = ? and " + b.tableName + "_id = ?"
	}
	memoryRelatedTable, err := GetMemoryTable(relatedTable)
	if err != nil {
		return 0, fmt.Errorf("memory table not found:" + relatedTable)
	}
	memoryTypedTable, err := GetMemoryTable(b.tableName)
	if err != nil {
		return 0, fmt.Errorf("memory table not found:" + relatedTable)
	}
	ids := make([]any, 2)
	adaptTrueFalseArgs(&whereRelatedArgs)
	if b.database == "" && len(databases) == 1 {
		whereRelatedTable = adaptConcatAndLen(whereRelatedTable, databases[0].Dialect)
	} else if b.database != "" {
		db, err := GetMemoryDatabase(b.database)
		if err == nil {
			whereRelatedTable = adaptConcatAndLen(whereRelatedTable, db.Dialect)
		}
	}
	adaptWhereQuery(&whereRelatedTable, relatedTable)
	data, err := Table(relatedTable).Where(whereRelatedTable, whereRelatedArgs...).One()
	if err != nil {
		return 0, err
	}
	if v, ok := data[memoryRelatedTable.Pk]; ok {
		if inOrder {
			ids[1] = v
		} else {
			ids[0] = v
		}
	}
	// get the typed model
	if b.whereQuery == "" {
		return 0, fmt.Errorf("you must specify a where for the typed struct")
	}
	typedModel, err := Table(b.tableName).Where(b.whereQuery, b.args...).One()
	if err != nil {
		return 0, err
	}
	if v, ok := typedModel[memoryTypedTable.Pk]; ok {
		if inOrder {
			ids[0] = v
		} else {
			ids[1] = v
		}
	}
	n, err := Table(relationTableName).Where(wherecols, ids...).Delete()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// GetRelated used for many to many to get related classes to a student or related students to a class
func (b *BuilderS[T]) GetRelated(relatedTable string, dest any) error {
	if b == nil || b.tableName == "" {
		return ErrTableNotFound
	}
	relationTableName := "m2m_" + b.tableName + "-" + b.database + "-" + relatedTable
	if _, ok := relationsMap.Get("m2m_" + b.tableName + "-" + b.database + "-" + relatedTable); !ok {
		relationTableName = "m2m_" + relatedTable + "-" + b.database + "-" + b.tableName
		if _, ok2 := relationsMap.Get("m2m_" + relatedTable + "-" + b.database + "-" + b.tableName); !ok2 {
			return fmt.Errorf("no relations many to many between theses 2 tables: %s, %s", b.tableName, relatedTable)
		}
	}

	if strings.HasPrefix(relationTableName, "m2m_"+b.tableName) {
		relationTableName = "m2m_" + b.tableName + "_" + relatedTable
	} else if strings.HasPrefix(relationTableName, "m2m_"+relatedTable) {
		relationTableName = "m2m_" + relatedTable + "_" + b.tableName
	}

	// get the typed model
	if b.whereQuery == "" {
		return fmt.Errorf("you must specify a where query like 'email = ? and username like ...' for structs")
	}
	b.whereQuery = strings.TrimSpace(b.whereQuery)
	if b.selected != "" {
		if !strings.Contains(b.selected, b.tableName) && !strings.Contains(b.selected, relatedTable) {
			if strings.Contains(b.selected, ",") {
				sp := strings.Split(b.selected, ",")
				for i := range sp {
					sp[i] = b.tableName + "." + sp[i]
				}
				b.selected = strings.Join(sp, ",")
			} else {
				b.selected = b.tableName + "." + b.selected
			}
		}
		b.statement = "SELECT " + b.selected + " FROM " + relatedTable
	} else {
		b.statement = "SELECT " + relatedTable + ".* FROM " + relatedTable
	}

	b.statement += " JOIN " + relationTableName + " ON " + relatedTable + ".id = " + relationTableName + "." + relatedTable + "_id"
	b.statement += " JOIN " + b.tableName + " ON " + b.tableName + ".id = " + relationTableName + "." + b.tableName + "_id"
	b.statement += " WHERE " + b.whereQuery
	if b.orderBys != "" {
		b.statement += " " + b.orderBys
	}
	if b.limit > 0 {
		i := strconv.Itoa(b.limit)
		b.statement += " LIMIT " + i
		if b.page > 0 {
			o := strconv.Itoa((b.page - 1) * b.limit)
			b.statement += " OFFSET " + o
		}
	}
	if b.debug {
		klog.Printf("statement:%s\n", b.statement)
		klog.Printf("args:%v\n", b.args)
	}
	err := Table(relationTableName).queryS(dest, b.statement, b.args...)
	if err != nil {
		return err
	}
	return nil
}

// JoinRelated same as get, but it join data
func (b *BuilderS[T]) JoinRelated(relatedTable string, dest any) error {
	if b == nil || b.tableName == "" {
		return ErrTableNotFound
	}
	relationTableName := "m2m_" + b.tableName + "-" + b.database + "-" + relatedTable
	if _, ok := relationsMap.Get("m2m_" + b.tableName + "-" + b.database + "-" + relatedTable); !ok {
		relationTableName = "m2m_" + relatedTable + "-" + b.database + "-" + b.tableName
		if _, ok2 := relationsMap.Get("m2m_" + relatedTable + "-" + b.database + "-" + b.tableName); !ok2 {
			return fmt.Errorf("no relations many to many between theses 2 tables: %s, %s", b.tableName, relatedTable)
		}
	}

	if strings.HasPrefix(relationTableName, "m2m_"+b.tableName) {
		relationTableName = "m2m_" + b.tableName + "_" + relatedTable
	} else if strings.HasPrefix(relationTableName, "m2m_"+relatedTable) {
		relationTableName = "m2m_" + relatedTable + "_" + b.tableName
	}

	// get the typed model
	if b.whereQuery == "" {
		return fmt.Errorf("you must specify a where for the typed struct")
	}
	b.whereQuery = strings.TrimSpace(b.whereQuery)
	if b.selected != "" {
		if !strings.Contains(b.selected, b.tableName) && !strings.Contains(b.selected, relatedTable) {
			if strings.Contains(b.selected, ",") {
				sp := strings.Split(b.selected, ",")
				for i := range sp {
					sp[i] = b.tableName + "." + sp[i]
				}
				b.selected = strings.Join(sp, ",")
			} else {
				b.selected = b.tableName + "." + b.selected
			}
		}
		b.statement = "SELECT " + b.selected + " FROM " + relatedTable
	} else {
		b.statement = "SELECT " + relatedTable + ".*," + b.tableName + ".* FROM " + relatedTable
	}

	b.statement += " JOIN " + relationTableName + " ON " + relatedTable + ".id = " + relationTableName + "." + relatedTable + "_id"
	b.statement += " JOIN " + b.tableName + " ON " + b.tableName + ".id = " + relationTableName + "." + b.tableName + "_id"
	if !strings.Contains(b.whereQuery, b.tableName) {
		return fmt.Errorf("you should specify table name like : %s.id = ? , instead of %s", b.tableName, b.whereQuery)
	}
	b.statement += " WHERE " + b.whereQuery
	if b.orderBys != "" {
		b.statement += " " + b.orderBys
	}
	if b.limit > 0 {
		i := strconv.Itoa(b.limit)
		b.statement += " LIMIT " + i
		if b.page > 0 {
			o := strconv.Itoa((b.page - 1) * b.limit)
			b.statement += " OFFSET " + o
		}
	}
	if b.debug {
		klog.Printf("statement:%s\n", b.statement)
		klog.Printf("args:%v\n", b.args)
	}
	err := Table(relationTableName).queryS(dest, b.statement, b.args...)
	if err != nil {
		return err
	}
	return nil
}

// Set used to update, Set("email,is_admin","example@mail.com",true) or Set("email = ? , is_admin = ?","example@mail.com",true)
func (b *BuilderS[T]) Set(query string, args ...any) (int, error) {
	if b == nil || b.tableName == "" {
		return 0, ErrTableNotFound
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "update",
		})
	}
	if onSet != nil {
		mToSet := map[string]any{}
		sp := strings.Split(query, ",")
		if strings.Contains(query, "?") {
			for i := range sp {
				sp[i] = setReplacer.Replace(sp[i])
				mToSet[strings.TrimSpace(sp[i])] = args[i]
			}
		} else {
			for i := range sp {
				mToSet[strings.TrimSpace(sp[i])] = args[i]
			}
		}
		err := onSet(b.database, b.tableName, mToSet)
		if err != nil {
			return 0, err
		}
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}

	if b.whereQuery == "" {
		return 0, errors.New("you should use Where before Update")
	}
	adaptSetQuery(&query)
	b.statement = "UPDATE " + b.tableName + " SET " + query + " WHERE " + b.whereQuery
	adaptTrueFalseArgs(&args)

	adaptPlaceholdersToDialect(&b.statement, db.Dialect)
	args = append(args, b.args...)
	if b.debug {
		klog.Printf("statement:%s\n", b.statement)
		klog.Printf("args:%v\n", args)
	}

	var res sql.Result
	if b.ctx != nil {
		res, err = db.Conn.ExecContext(b.ctx, b.statement, args...)
	} else {
		res, err = db.Conn.Exec(b.statement, args...)
	}
	if err != nil {
		return 0, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(aff), nil
}

// Delete data from database, can be multiple, depending on the where, return affected rows(Not every database or database driver may support affected rows)
func (b *BuilderS[T]) Delete() (int, error) {
	if b == nil || b.tableName == "" {
		return 0, ErrTableNotFound
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "delete",
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}
	if onDelete != nil {
		err := onDelete(b.database, b.tableName, b.whereQuery, b.args...)
		if err != nil {
			return 0, err
		}
	}

	b.statement = "DELETE FROM " + b.tableName
	if b.whereQuery != "" {
		b.statement += " WHERE " + b.whereQuery
	} else {
		return 0, errors.New("no Where was given for this query:" + b.whereQuery)
	}
	adaptPlaceholdersToDialect(&b.statement, db.Dialect)
	if b.debug {
		klog.Printf("statement:%s\n", b.statement)
		klog.Printf("args:%v\n", b.args)
	}

	var res sql.Result

	if b.ctx != nil {
		res, err = db.Conn.ExecContext(b.ctx, b.statement, b.args...)
	} else {
		res, err = db.Conn.Exec(b.statement, b.args...)
	}
	if err != nil {
		return 0, err
	}
	affectedRows, err := res.RowsAffected()
	if err != nil {
		return int(affectedRows), err
	}
	return int(affectedRows), nil
}

// Drop drop table from db
func (b *BuilderS[T]) Drop() (int, error) {
	if b == nil || b.tableName == "" {
		return 0, ErrTableNotFound
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "drop",
		})
	}
	if onDrop != nil {
		err := onDrop(b.database, b.tableName)
		if err != nil {
			return 0, err
		}
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}

	b.statement = "DROP TABLE " + b.tableName
	var res sql.Result
	if b.ctx != nil {
		res, err = db.Conn.ExecContext(b.ctx, b.statement)
	} else {
		res, err = db.Conn.Exec(b.statement)
	}
	if err != nil {
		return 0, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return int(aff), err
	}
	return int(aff), err
}

// Select usage: Select("email","password")
func (b *BuilderS[T]) Select(columns ...string) *BuilderS[T] {
	if b == nil || b.tableName == "" {
		return nil
	}
	b.selected = strings.Join(columns, ",")
	b.order = append(b.order, "select")
	return b
}

// Where can be like : Where("id > ?",1) or Where("id",1) = Where("id = ?",1)
func (b *BuilderS[T]) Where(query string, args ...any) *BuilderS[T] {
	if b == nil || b.tableName == "" {
		return nil
	}
	if b.database == "" && len(databases) == 1 {
		query = adaptConcatAndLen(query, databases[0].Dialect)
	} else if b.database != "" {
		db, err := GetMemoryDatabase(b.database)
		if err == nil {
			query = adaptConcatAndLen(query, db.Dialect)
			b.database = db.Name
		}
	}
	adaptWhereQuery(&query, b.tableName)
	adaptTrueFalseArgs(&args)
	b.whereQuery = query
	b.args = append(b.args, args...)
	b.order = append(b.order, "where")
	return b
}

// Limit set limit
func (b *BuilderS[T]) Limit(limit int) *BuilderS[T] {
	if b == nil || b.tableName == "" {
		return nil
	}
	b.limit = limit
	b.order = append(b.order, "limit")
	return b
}

// Context allow to query or execute using ctx
func (b *BuilderS[T]) Context(ctx context.Context) *BuilderS[T] {
	if b == nil || b.tableName == "" {
		return nil
	}
	b.ctx = ctx
	return b
}

// Page return paginated elements using Limit for specific page
func (b *BuilderS[T]) Page(pageNumber int) *BuilderS[T] {
	if b == nil || b.tableName == "" {
		return nil
	}
	b.page = pageNumber
	b.order = append(b.order, "page")
	return b
}

// OrderBy can be used like: OrderBy("-id","-email") OrderBy("id","-email") OrderBy("+id","email")
func (b *BuilderS[T]) OrderBy(fields ...string) *BuilderS[T] {
	if b == nil || b.tableName == "" {
		return nil
	}
	b.orderBys = "ORDER BY "
	orders := []string{}

	for _, f := range fields {
		addTableName := false
		if b.tableName != "" {
			if !strings.Contains(f, b.tableName) {
				addTableName = true
			}
		}
		if strings.HasPrefix(f, "+") {
			if addTableName {
				orders = append(orders, b.tableName+"."+f[1:]+" ASC")
			} else {
				orders = append(orders, f[1:]+" ASC")
			}
		} else if strings.HasPrefix(f, "-") {
			if addTableName {
				orders = append(orders, b.tableName+"."+f[1:]+" DESC")
			} else {
				orders = append(orders, f[1:]+" DESC")
			}
		} else {
			if addTableName {
				orders = append(orders, b.tableName+"."+f+" ASC")
			} else {
				orders = append(orders, f+" ASC")
			}
		}
	}
	b.orderBys += strings.Join(orders, ",")
	b.order = append(b.order, "order_by")
	return b
}

// Debug print prepared statement and values for this operation
func (b *BuilderS[T]) Debug() *BuilderS[T] {
	if b == nil || b.tableName == "" {
		return nil
	}
	b.debug = true
	return b
}

// All get all data
func (b *BuilderS[T]) All() ([]T, error) {
	if b == nil || b.tableName == "" {
		return nil, ErrTableNotFound
	}
	c := dbCache{
		database:   b.database,
		table:      b.tableName,
		selected:   b.selected,
		statement:  b.statement,
		orderBys:   b.orderBys,
		whereQuery: b.whereQuery,
		offset:     b.offset,
		limit:      b.limit,
		page:       b.page,
		args:       fmt.Sprint(b.args...),
	}
	if useCache {
		if v, ok := cacheAllS.Get(c); ok {
			return v.([]T), nil
		}
	}
	if b.selected != "" && b.selected != "*" {
		b.statement = "select " + b.selected + " from " + b.tableName
	} else {
		b.statement = "select * from " + b.tableName
	}

	if b.whereQuery != "" {
		b.statement += " WHERE " + b.whereQuery
	}

	if b.orderBys != "" {
		b.statement += " " + b.orderBys
	}

	if b.limit > 0 {
		i := strconv.Itoa(b.limit)
		b.statement += " LIMIT " + i
		if b.page > 0 {
			o := strconv.Itoa((b.page - 1) * b.limit)
			b.statement += " OFFSET " + o
		}
	}

	if b.debug {
		klog.Printf("statement:%s\n", b.statement)
		klog.Printf("args:%v\n", b.args)
	}

	models, err := b.queryS(b.statement, b.args...)
	if err != nil {
		return nil, err
	}
	if useCache {
		_ = cacheAllS.Set(c, models)
	}
	return models, nil
}

// One get single row
func (b *BuilderS[T]) One() (T, error) {
	if b == nil || b.tableName == "" {
		return *new(T), ErrTableNotFound
	}
	c := dbCache{
		database:   b.database,
		table:      b.tableName,
		selected:   b.selected,
		statement:  b.statement,
		orderBys:   b.orderBys,
		whereQuery: b.whereQuery,
		offset:     b.offset,
		limit:      b.limit,
		page:       b.page,
		args:       fmt.Sprint(b.args...),
	}
	if useCache {
		if v, ok := cacheOneS.Get(c); ok {
			return v.(T), nil
		}
	}
	if b.tableName == "" {
		return *new(T), errors.New("unable to find model, try korm.LinkModel before")
	}

	if b.selected != "" && b.selected != "*" {
		b.statement = "select " + b.selected + " from " + b.tableName
	} else {
		b.statement = "select * from " + b.tableName
	}

	if b.whereQuery != "" {
		b.statement += " WHERE " + b.whereQuery
	}

	if b.orderBys != "" {
		b.statement += " " + b.orderBys
	}

	if b.limit > 0 {
		i := strconv.Itoa(b.limit)
		b.statement += " LIMIT " + i
	} else {
		b.statement += " LIMIT 1"
	}

	if b.debug {
		klog.Printf("statement:%s\n", b.statement)
		klog.Printf("args:%v\n", b.args)
	}

	model, err := b.queryOneS(b.statement, b.args...)
	if err != nil {
		return *new(T), err
	}
	if useCache {
		_ = cacheOneS.Set(c, model)
	}
	return model, nil
}

func (b *BuilderS[T]) queryS(query string, args ...any) ([]T, error) {
	if b == nil || b.tableName == "" {
		return nil, ErrTableNotFound
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return nil, err
	}
	if db.Conn == nil {
		return nil, errors.New("no connection")
	}
	adaptPlaceholdersToDialect(&query, db.Dialect)
	adaptTrueFalseArgs(&args)
	res := make([]T, 0)
	var rows *sql.Rows
	if b.ctx != nil {
		rows, err = db.Conn.QueryContext(b.ctx, query, args...)
	} else {
		rows, err = db.Conn.Query(query, args...)
	}

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no data found")
	} else if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	if b.selected != "" && b.selected != "*" {
		cols = strings.Split(b.selected, ",")
	} else {
		cols, err = rows.Columns()
		if err != nil {
			return nil, err
		}
	}

	columns_ptr_to_values := make([]any, len(cols))
	values := make([]any, len(cols))
	for rows.Next() {
		for i := range values {
			columns_ptr_to_values[i] = &values[i]
		}

		err := rows.Scan(columns_ptr_to_values...)
		if err != nil {
			return nil, err
		}

		row := new(T)

		m := map[string]any{}
		if b.selected != "" && b.selected != "*" {
			for i, key := range strings.Split(b.selected, ",") {
				if db.Dialect == MYSQL || db.Dialect == MARIA {
					if v, ok := values[i].([]byte); ok {
						values[i] = string(v)
					}
				}
				m[key] = values[i]
			}
		} else {
			for i, key := range cols {
				if db.Dialect == MYSQL || db.Dialect == MARIA {
					if v, ok := values[i].([]byte); ok {
						values[i] = string(v)
					}
				}
				m[key] = values[i]
			}
		}
		err = kstrct.FillFromMap(row, m)
		if err != nil {
			return nil, err
		}
		res = append(res, *row)
	}

	if len(res) == 0 {
		return nil, errors.New("no data found")
	}
	return res, nil
}

func (b *BuilderS[T]) queryOneS(query string, args ...any) (res T, err error) {
	if b == nil || b.tableName == "" {
		return res, ErrTableNotFound
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return res, err
	}

	adaptPlaceholdersToDialect(&query, db.Dialect)
	adaptTrueFalseArgs(&args)
	if db.Conn == nil {
		return res, errors.New("no connection")
	}
	var rows *sql.Rows
	if b.ctx != nil {
		rows, err = db.Conn.QueryContext(b.ctx, query, args...)
	} else {
		rows, err = db.Conn.Query(query, args...)
	}

	if err == sql.ErrNoRows {
		return res, fmt.Errorf("no data found")
	} else if err != nil {
		return res, err
	}
	defer rows.Close()

	var cols []string
	if b.selected != "" && b.selected != "*" {
		cols = strings.Split(b.selected, ",")
	} else {
		cols, err = rows.Columns()
		if err != nil {
			return res, err
		}
	}

	columns_ptr_to_values := make([]any, len(cols))
	values := make([]any, len(cols))
	for rows.Next() {
		for i := range values {
			columns_ptr_to_values[i] = &values[i]
		}

		err := rows.Scan(columns_ptr_to_values...)
		if err != nil {
			return res, err
		}

		m := map[string]any{}
		if b.selected != "" && b.selected != "*" {
			for i, key := range strings.Split(b.selected, ",") {
				if db.Dialect == MYSQL || db.Dialect == MARIA {
					if v, ok := values[i].([]byte); ok {
						values[i] = string(v)
					}
				}
				m[key] = values[i]
			}
		} else {
			for i, key := range cols {
				if db.Dialect == MYSQL || db.Dialect == MARIA {
					if v, ok := values[i].([]byte); ok {
						values[i] = string(v)
					}
				}
				m[key] = values[i]
			}
		}
		err = kstrct.FillFromMap(&res, m)
		if err != nil {
			return res, err
		}
	}
	if res == *new(T) {
		return res, fmt.Errorf("no data")
	}
	return res, nil
}
