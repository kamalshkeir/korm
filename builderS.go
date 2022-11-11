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
	"github.com/kamalshkeir/korm/drivers/kmongo"
	"github.com/kamalshkeir/kstrct"
)

var cachesOneS = kmap.New[dbCache, any](false)
var cachesAllS = kmap.New[dbCache, any](false)

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
			klog.Printf("rdunable to find tableName from model, restart the app if you just migrated\n")
			return nil
		}
	}
	return &Builder[T]{
		tableName: tName,
	}
}

func BuilderS[T comparable](tableName ...string) *Builder[T] {
	tName := getTableName[T]()
	if tName == "" {
		if len(tableName) > 0 {
			mModelTablename[*new(T)] = tableName[0]
			tName = tableName[0]
		} else {
			klog.Printf("rdunable to find tableName from model, restart the app if you just migrated\n")
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
	if klog.CheckError(err) {
		b.database = databases[0].Name
	} else {
		b.database = db.Name
	}
	return b
}

func (b *Builder[T]) Insert(model *T) (int, error) {
	if b.tableName == "" {
		tName := getTableName[T]()
		if tName == "" {
			return 0, errors.New("unable to find tableName from model, restart the app if you just migrated")
		}
		b.tableName = tName
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		go cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "create",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}
	if db.Dialect == MONGO {
		if b.ctx == nil {
			b.ctx = context.Background()
		}
		err := kmongo.CreateRow(b.ctx, b.tableName, model, b.database)
		if klog.CheckError(err) {
			return 0, err
		}
		return 1, nil
	}

	names, mvalues, _, mtags := getStructInfos(model)
	values := []any{}
	if len(names) < len(mvalues) {
		return 0, errors.New("there is more values than fields")
	}
	placeholdersSlice := []string{}
	index := 999
	for i, name := range names {
		if v, ok := mvalues[name]; ok {
			values = append(values, v)
		} else {
			klog.Printf("rd%vnot found in fields\n")
			return 0, errors.New("field not found")
		}
		if SliceContains(mtags[name], "autoinc", "pk") || (strings.Contains(name, "id") && i == 0) {
			index = i
		} else {
			placeholdersSlice = append(placeholdersSlice, "?")
		}
	}
	if index != 999 {
		names = append(names[:index], names[index+1:]...)
		values = append(values[:index], values[index+1:]...)
		delete(mvalues, names[index])
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
	if err != nil {
		if Debug {
			klog.Printf("%s,%s\n", b.statement, values)
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

// Set usage: Set("email = ? AND is_admin = ?","example@mail.com",true)
func (b *Builder[T]) Set(query string, args ...any) (int, error) {
	if b.tableName == "" {
		tName := getTableName[T]()
		if tName == "" {
			klog.Printf("rdunable to find tableName from model\n")
			return 0, errors.New("unable to find tableName from model")
		}
		b.tableName = tName
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		go cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "update",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}

	if db.Dialect == MONGO {
		if b.ctx == nil {
			b.ctx = context.Background()
		}
		wf := map[string]any{}
		if b.whereQuery != "" {
			r := strings.NewReplacer("?", "", "=", "", "AND", ",", "and", ",", "OR", ",", "or", ",")
			b.whereQuery = r.Replace(b.whereQuery)
			if strings.Contains(b.whereQuery, ",") {
				sp := strings.Split(b.whereQuery, ",")
				if len(b.args) == len(sp) {
					for i, s := range sp {
						wf[strings.TrimSpace(s)] = b.args[i]
					}
				}
			} else {
				if len(b.args) == 1 {
					wf[strings.TrimSpace(b.whereQuery)] = b.args[0]
				}
			}
		}
		newRow := map[string]any{}
		spp := strings.Split(query, ",")
		for _, s := range spp {
			seq := strings.Split(s, "=")
			newRow[seq[0]] = seq[1]
		}
		err := kmongo.UpdateRow(b.ctx, b.tableName, wf, newRow)
		if klog.CheckError(err) {
			return 0, err
		}
		return 1, nil
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
		if Debug {
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
	if b.tableName == "" {
		tName := getTableName[T]()
		if tName == "" {
			klog.Printf("unable to find tableName from model\n")
			return 0, errors.New("unable to find tableName from model")
		}
		b.tableName = tName
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		go cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "delete",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}
	if db.Dialect == MONGO {
		wf := map[string]any{}
		if b.whereQuery != "" {
			r := strings.NewReplacer("?", "", "=", "", "AND", ",", "and", ",", "OR", ",", "or", ",")
			b.whereQuery = r.Replace(b.whereQuery)
			if strings.Contains(b.whereQuery, ",") {
				sp := strings.Split(b.whereQuery, ",")
				if len(b.args) == len(sp) {
					for i, s := range sp {
						wf[strings.TrimSpace(s)] = b.args[i]
					}
				}
			} else {
				if len(b.args) == 1 {
					wf[strings.TrimSpace(b.whereQuery)] = b.args[0]
				}
			}
		}
		if b.ctx == nil {
			b.ctx = context.Background()
		}
		err := kmongo.DeleteRow(b.ctx, b.tableName, wf, b.database)
		if klog.CheckError(err) {
			return 0, err
		}
		return 1, nil
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
	if b.tableName == "" {
		tName := getTableName[T]()
		if tName == "" {
			return 0, errors.New("unable to find tableName from model")
		}
		b.tableName = tName
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		go cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "drop",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}
	if db.Dialect == MONGO {
		if b.ctx == nil {
			b.ctx = context.Background()
		}
		kmongo.DropTable(b.ctx, b.tableName, b.database)
		return 1, nil
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
	if b.database == "" {
		if databases[0].Dialect == MONGO {
			b.database = databases[0].Name
		}
	}
	if _, ok := kmongo.MMongoDBS.Get(b.database); ok {
		b.orderBys = strings.Join(fields, ",")
		b.order = append(b.order, "order_by")
		return b
	}
	b.orderBys = "ORDER BY "
	orders := []string{}
	for _, f := range fields {
		if strings.HasPrefix(f, "+") {
			orders = append(orders, f[1:]+" ASC")
		} else if strings.HasPrefix(f, "-") {
			orders = append(orders, f[1:]+" DESC")
		} else {
			orders = append(orders, f+" ASC")
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
	if b.database == "" {
		b.database = databases[0].Name
	}
	if b.tableName == "" {
		return nil, errors.New("error: this model is not linked, execute korm.AutoMigrate before")
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
	if _, ok := kmongo.MMongoDBS.Get(b.database); ok {
		wf := map[string]any{}
		if b.whereQuery != "" {
			r := strings.NewReplacer("?", "", "=", "", "AND", ",", "and", ",", "OR", ",", "or", ",")
			b.whereQuery = r.Replace(b.whereQuery)
			if strings.Contains(b.whereQuery, ",") {
				sp := strings.Split(b.whereQuery, ",")
				if len(b.args) == len(sp) {
					for i, s := range sp {
						wf[strings.TrimSpace(s)] = b.args[i]
					}
				}
			} else {
				if len(b.args) == 1 {
					wf[strings.TrimSpace(b.whereQuery)] = b.args[0]
				}
			}
		}

		if len(wf) == 0 {
			wf = nil
		}
		if b.ctx == nil {
			b.ctx = context.Background()
		}
		data, err := kmongo.Query[T](b.ctx, b.tableName, b.selected, wf, int64(b.limit), int64(b.page), b.orderBys, b.database)
		if err != nil {
			return nil, err
		}
		if useCache {
			cachesAllS.Set(c, data)
		}
		return data, nil
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
	if b.database == "" {
		b.database = databases[0].Name
	}
	if b.tableName == "" {
		return *new(T), errors.New("error: this model is not linked, execute korm.AutoMigrate first")
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
	if _, ok := kmongo.MMongoDBS.Get(b.database); ok {
		wf := map[string]any{}
		if b.whereQuery != "" {
			r := strings.NewReplacer("?", "", "=", "", "AND", ",", "and", ",", "OR", ",", "or", ",")
			b.whereQuery = r.Replace(b.whereQuery)
			if strings.Contains(b.whereQuery, ",") {
				sp := strings.Split(b.whereQuery, ",")
				if len(b.args) == len(sp) {
					for i, s := range sp {
						wf[strings.TrimSpace(s)] = b.args[i]
					}
				}
			} else {
				if len(b.args) == 1 {
					wf[strings.TrimSpace(b.whereQuery)] = b.args[0]
				}
			}
		}
		if len(wf) == 0 {
			wf = nil
		}
		if b.ctx == nil {
			b.ctx = context.Background()
		}
		data, err := kmongo.QueryOne[T](b.ctx, b.tableName, b.selected, wf, int64(b.limit), int64(b.page), strings.ReplaceAll(b.orderBys, "ORDER BY", ""), b.database)
		if err != nil {
			return data, err
		}
		if useCache {
			cachesOneS.Set(c, data)
		}
		return data, nil
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
	if b.database == "" {
		b.database = databases[0].Name
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return nil, err
	}

	adaptPlaceholdersToDialect(&query, db.Dialect)
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
			for i := range values {
				if v, ok := values[i].([]byte); ok {
					values[i] = string(v)
				}
			}
		}

		row := new(T)
		if b.selected != "" && b.selected != "*" {
			kstrct.FillFromSelected(row, b.selected, values...)
		} else {
			kstrct.FillFromValues(row, values...)
		}
		res = append(res, *row)
	}

	if len(res) == 0 {
		return nil, errors.New("no data found")
	}
	return res, nil
}
