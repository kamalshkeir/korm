package korm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/kstrct"
)

var cachesOneS = kmap.New[dbCache, any](false)
var cachesAllS = kmap.New[dbCache, any](false)
var errModelNotFound = errors.New("unable to find tableName from model, restart the app if you just migrated")

type Builder[T comparable] struct {
	debug      bool
	limit      int
	page       int
	tableName  string
	selected   string
	orderBys   string
	whereQuery string
	query      string
	offset     string
	statement  string
	database   string
	args       []any
	order      []string
	ctx        context.Context
}

func Model[T comparable](tableName ...string) *Builder[T] {
	tName := getTableName[T]()
	if tName == "" {
		if len(tableName) > 0 {
			mModelTablename[*new(T)] = tableName[0]
			tName = tableName[0]
		} else {
			return nil
		}
	}
	return &Builder[T]{
		tableName: tName,
	}
}

func BuilderStruct[T comparable](tableName ...string) *Builder[T] {
	tName := getTableName[T]()
	if tName == "" {
		if len(tableName) > 0 {
			mModelTablename[*new(T)] = tableName[0]
			tName = tableName[0]
		} else {
			return nil
		}
	}
	return &Builder[T]{
		tableName: tName,
	}
}

func (b *Builder[T]) Database(dbName string) *Builder[T] {
	b.database = dbName
	if b.database == "" {
		b.database = databases[0].Name
	}
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
		b.database = databases[0].Name
	} else {
		b.database = db.Name
	}
	return b
}

func (b *Builder[T]) Insert(model *T) (int, error) {
	if b == nil || b.tableName == "" {
		return 0, errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
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

	names, mvalues, _, mtags := getStructInfos(model, true)
	values := []any{}
	if len(names) < len(mvalues) {
		return 0, errors.New("more values than fields")
	}
	placeholdersSlice := []string{}
	ignored := []int{}
	for i, name := range names {
		if v, ok := mvalues[name]; ok {
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
					ig = true
				default:
					if strings.Contains(tag, "m2m") {
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
	var affectedRows int
	stat := strings.Builder{}
	stat.WriteString("INSERT INTO " + b.tableName + " (")
	stat.WriteString(fields_comma_separated)
	stat.WriteString(") VALUES (")
	stat.WriteString(placeholders)
	stat.WriteString(")")
	b.statement = stat.String()
	adaptPlaceholdersToDialect(&b.statement, db.Dialect)
	var res sql.Result
	if b.ctx != nil {
		res, err = db.Conn.ExecContext(b.ctx, b.statement, values...)
	} else {
		res, err = db.Conn.Exec(b.statement, values...)
	}

	if b.debug {
		klog.Printf("%s,%s\n", b.statement, values)
	}
	if err != nil {
		if b.debug {
			klog.Printf("rderr:%v\n", err)
		}
		return affectedRows, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return int(rows), err
	}
	return int(rows), nil
}

func (b *Builder[T]) AddRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error) {
	if b == nil || b.tableName == "" {
		return 0, errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
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
	ids := make([]any, 4)

	data, err := Table(relatedTable).Where(whereRelatedTable, whereRelatedArgs...).One()
	if err != nil {
		return 0, err
	}
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
	stat := "INSERT INTO " + relationTableName + "(" + cols + ") SELECT ?,? WHERE NOT EXISTS (SELECT * FROM " + relationTableName + " WHERE " + wherecols + ");"
	adaptPlaceholdersToDialect(&stat, db.Dialect)
	err = Exec(b.database, stat, ids...)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (b *Builder[T]) DeleteRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error) {
	if b == nil || b.tableName == "" {
		return 0, errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
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

func (b *Builder[T]) GetRelated(relatedTable string, dest any) error {
	if b == nil || b.tableName == "" {
		return errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
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

func (b *Builder[T]) JoinRelated(relatedTable string, dest any) error {
	if b == nil || b.tableName == "" {
		return errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
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

// Set usage: Set("email = ? AND is_admin = ?","example@mail.com",true)
func (b *Builder[T]) Set(query string, args ...any) (int, error) {
	if b == nil || b.tableName == "" {
		return 0, errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "update",
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}

	if b.whereQuery == "" {
		return 0, errors.New("you should use Where before Update")
	}

	b.statement = "UPDATE " + b.tableName + " SET " + query + " WHERE " + b.whereQuery
	adaptPlaceholdersToDialect(&b.statement, db.Dialect)
	args = append(args, b.args...)
	if b.debug {
		klog.Printf("statement:%s_n", b.statement)
		klog.Printf("args:%v\n", b.args)
	}

	var res sql.Result
	if b.ctx != nil {
		res, err = db.Conn.ExecContext(b.ctx, b.statement, args...)
	} else {
		res, err = db.Conn.Exec(b.statement, args...)
	}
	if err != nil {
		if b.debug {
			klog.Printf("%s,%s\n", b.statement, args)
			klog.Printf("rd%v\n", err)
		}
		return 0, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(aff), nil
}

func (b *Builder[T]) Delete() (int, error) {
	if b == nil || b.tableName == "" {
		return 0, errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
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

func (b *Builder[T]) Drop() (int, error) {
	if b == nil || b.tableName == "" {
		return 0, errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "drop",
		})
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
func (b *Builder[T]) Select(columns ...string) *Builder[T] {
	s := []string{}
	s = append(s, columns...)
	b.selected = strings.Join(s, ",")
	b.order = append(b.order, "select")
	return b
}

func (b *Builder[T]) Where(query string, args ...any) *Builder[T] {
	b.whereQuery = query
	if strings.Contains(query, ",") {
		sp := strings.Split(query, ",")
		for i := range sp {
			if !strings.HasPrefix(sp[i], b.tableName) {
				sp[i] = b.tableName + "." + sp[i] + " = ?"
				if !strings.Contains(query, "?") {
					sp[i] += " = ?"
				}
			}
		}
		b.whereQuery = strings.Join(sp, ",")
	} else {
		if !strings.HasPrefix(query, b.tableName) {
			b.whereQuery = b.tableName + "." + query
		}
		if !strings.Contains(query, "?") {
			b.whereQuery += " = ?"
		}
	}

	b.args = append(b.args, args...)
	b.order = append(b.order, "where")
	return b
}

func (b *Builder[T]) Query(query string, args ...any) *Builder[T] {
	b.query = query
	b.args = append(b.args, args...)
	b.order = append(b.order, "query")
	return b
}

func (b *Builder[T]) Limit(limit int) *Builder[T] {
	b.limit = limit
	b.order = append(b.order, "limit")
	return b
}

func (b *Builder[T]) Context(ctx context.Context) *Builder[T] {
	b.ctx = ctx
	return b
}

func (b *Builder[T]) Page(pageNumber int) *Builder[T] {
	b.page = pageNumber
	b.order = append(b.order, "page")
	return b
}

func (b *Builder[T]) OrderBy(fields ...string) *Builder[T] {
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

func (b *Builder[T]) Debug() *Builder[T] {
	b.debug = true
	return b
}

func (b *Builder[T]) All() ([]T, error) {
	if b == nil || b.tableName == "" {
		return nil, errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	c := dbCache{
		database:   b.database,
		table:      b.tableName,
		selected:   b.selected,
		statement:  b.statement,
		orderBys:   b.orderBys,
		whereQuery: b.whereQuery,
		query:      b.query,
		offset:     b.offset,
		limit:      b.limit,
		page:       b.page,
		args:       fmt.Sprintf("%v", b.args...),
	}
	if useCache {
		if v, ok := cachesAllS.Get(c); ok {
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
	if b.query != "" {
		b.limit = 0
		b.orderBys = ""
		b.statement = b.query
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
		cachesAllS.Set(c, models)
	}
	return models, nil
}

func (b *Builder[T]) One() (T, error) {
	if b == nil || b.tableName == "" {
		return *new(T), errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	c := dbCache{
		database:   b.database,
		table:      b.tableName,
		selected:   b.selected,
		statement:  b.statement,
		orderBys:   b.orderBys,
		whereQuery: b.whereQuery,
		query:      b.query,
		offset:     b.offset,
		limit:      b.limit,
		page:       b.page,
		args:       fmt.Sprintf("%v", b.args...),
	}
	if useCache {
		if v, ok := cachesOneS.Get(c); ok {
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

	if b.query != "" {
		b.limit = 0
		b.orderBys = ""
		b.statement = b.query
	}

	if b.orderBys != "" {
		b.statement += " " + b.orderBys
	}

	if b.limit > 0 {
		i := strconv.Itoa(b.limit)
		b.statement += " LIMIT " + i
	}

	if b.debug {
		klog.Printf("statement:%s\n", b.statement)
		klog.Printf("args:%v\n", b.args)
	}

	models, err := b.queryS(b.statement, b.args...)
	if err != nil {
		return *new(T), err
	}
	if useCache {
		cachesOneS.Set(c, models[0])
	}
	return models[0], nil
}

func (b *Builder[T]) queryS(query string, args ...any) ([]T, error) {
	if b == nil || b.tableName == "" {
		return nil, errModelNotFound
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return nil, err
	}

	adaptPlaceholdersToDialect(&query, db.Dialect)
	res := make([]T, 0)

	stmt, err := db.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var rows *sql.Rows
	if b.ctx != nil {
		rows, err = stmt.QueryContext(b.ctx, args...)
	} else {
		rows, err = stmt.Query(args...)
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

	columns_ptr_to_values := make([]interface{}, len(cols))
	values := make([]interface{}, len(cols))
	for rows.Next() {
		for i := range values {
			columns_ptr_to_values[i] = &values[i]
		}

		err := rows.Scan(columns_ptr_to_values...)
		if err != nil {
			return nil, err
		}

		if db.Dialect == MYSQL || db.Dialect == MARIA || db.Dialect == "mariadb" {
			db.Dialect = MYSQL
			for i := range values {
				if v, ok := values[i].([]byte); ok {
					values[i] = string(v)
				}
			}
		}

		row := new(T)

		m := map[string]any{}
		if b.selected != "" && b.selected != "*" {
			for i, key := range strings.Split(b.selected, ",") {
				m[key] = values[i]
			}
		} else {
			for i, key := range cols {
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
