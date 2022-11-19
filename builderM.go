package korm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/kamalshkeir/klog"
)


type BuilderM struct {
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

func Table(tableName string) *BuilderM {
	return &BuilderM{
		tableName: tableName,
	}
}

func BuilderMap(tableName string) *BuilderM {
	return &BuilderM{
		tableName: tableName,
	}
}

func (b *BuilderM) Database(dbName string) *BuilderM {
	b.database = dbName
	return b
}

func (b *BuilderM) Select(columns ...string) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse .Table before .Select\n")
		return nil
	}
	s := []string{}
	s = append(s, columns...)
	b.selected = strings.Join(s, ",")
	b.order = append(b.order, "select")
	return b
}

func (b *BuilderM) Where(query string, args ...any) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse .Table before .Where\n")
		return nil
	}
	b.whereQuery = query
	b.args = append(b.args, args...)
	b.order = append(b.order, "where")
	return b
}

func (b *BuilderM) Query(query string, args ...any) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Query\n")
		return nil
	}
	b.query = query
	b.args = append(b.args, args...)
	b.order = append(b.order, "query")
	return b
}

func (b *BuilderM) Limit(limit int) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Limit\n")
		return nil
	}
	b.limit = limit
	b.order = append(b.order, "limit")
	return b
}

func (b *BuilderM) Page(pageNumber int) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Page\n")
		return nil
	}
	b.page = pageNumber
	b.order = append(b.order, "page")
	return b
}

func (b *BuilderM) OrderBy(fields ...string) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before OrderBy\n")
		return nil
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

func (b *BuilderM) Context(ctx context.Context) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Context\n")
		return nil
	}
	b.ctx = ctx
	return b
}

func (b *BuilderM) Debug() *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Debug\n")
		return nil
	}
	b.debug = true
	return b
}

func (b *BuilderM) All() ([]map[string]any, error) {
	if b.tableName == "" {
		return nil, errors.New("unable to find table, try db.Table before")
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
		if v, ok := cachesAllM.Get(c); ok {
			return v, nil
		}
	}
	if b.database == "" {
		b.database = databases[0].Name
	}

	if b.selected != "" {
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

	models, err := b.queryM(b.statement, b.args...)
	if err != nil {
		return nil, err
	}

	if useCache {
		cachesAllM.Set(c, models)
	}
	return models, nil
}

func (b *BuilderM) One() (map[string]any, error) {
	if b.tableName == "" {
		return nil, errors.New("unable to find table, try db.Table before")
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
		if v, ok := cachesOneM.Get(c); ok {
			return v, nil
		}
	}
	if b.database == "" {
		b.database = databases[0].Name
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
	}

	if b.debug {
		klog.Printf("ylstatement:%s\n", b.statement)
		klog.Printf("ylargs:%v\n", b.args)
	}

	models, err := b.queryM(b.statement, b.args...)
	if err != nil {
		return nil, err
	}

	if len(models) == 0 {
		return nil, errors.New("no data")
	}
	if useCache {
		cachesOneM.Set(c, models[0])
	}

	return models[0], nil
}

func (b *BuilderM) Insert(fields_comma_separated string, fields_values ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find table, try db.Table before")
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
	if err != nil {
		return 0, err
	}

	split := strings.Split(fields_comma_separated, ",")
	if len(split) != len(fields_values) {
		return 0, errors.New("fields and fields_values doesn't have the same length")
	}
	placeholdersSlice := []string{}
	for i := range split {
		switch db.Dialect {
		case POSTGRES, SQLITE:
			placeholdersSlice = append(placeholdersSlice, "$"+strconv.Itoa(i+1))
		case MYSQL, MARIA, "mariadb":
			placeholdersSlice = append(placeholdersSlice, "?")
		default:
			return 0, errors.New("database is neither sqlite, postgres or mysql")
		}
	}
	placeholders := strings.Join(placeholdersSlice, ",")
	var affectedRows int

	stat := strings.Builder{}
	stat.WriteString("INSERT INTO " + b.tableName + " (")
	stat.WriteString(fields_comma_separated)
	stat.WriteString(") VALUES (")
	stat.WriteString(placeholders)
	stat.WriteString(")")
	statement := stat.String()
	if b.debug {
		klog.Printf("ylstatement:%s\n", b.statement)
		klog.Printf("ylargs:%v\n", b.args)
	}
	var res sql.Result
	if b.ctx != nil {
		res, err = db.Conn.ExecContext(b.ctx, statement, fields_values...)
	} else {
		res, err = db.Conn.Exec(statement, fields_values...)
	}
	if err != nil {
		if Debug {
			klog.Printf("ylstatement: %s\nfields_values: %v \n", statement, fields_values)
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

func (b *BuilderM) Set(query string, args ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try db.Table before")
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
	if err != nil {
		return 0, err
	}
	if b.whereQuery == "" {
		return 0, errors.New("you should use Where before Update")
	}

	b.statement = "UPDATE " + b.tableName + " SET " + query + " WHERE " + b.whereQuery
	adaptPlaceholdersToDialect(&b.statement, db.Dialect)
	args = append(args, b.args...)
	if b.debug {
		klog.Printf("ylstatement:%s\n", b.statement)
		klog.Printf("ylargs:%v\n", b.args)
	}

	var res sql.Result
	if b.ctx != nil {
		res, err = db.Conn.ExecContext(b.ctx, b.statement, args...)
	} else {
		res, err = db.Conn.Exec(b.statement, args...)
	}
	if err != nil {
		if Debug {
			klog.Printf("ylstatement:%s\n", b.statement)
			klog.Printf("ylargs:%v\n", b.args)
			klog.Printf("reerror:%v\n", err)
		}
		return 0, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(aff), nil
}

func (b *BuilderM) Delete() (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try korm.AutoMigrate before")
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
	if err != nil {
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
		klog.Printf("ylstatement:%s\n", b.statement)
		klog.Printf("ylargs:%v\n", b.args)
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

func (b *BuilderM) Drop() (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try korm.LinkModel before Update")
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "drop",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
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

func (b *BuilderM) queryM(statement string, args ...any) ([]map[string]interface{}, error) {
	if b.database == "" {
		b.database = databases[0].Name
	}
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
		return nil, err
	}
	adaptPlaceholdersToDialect(&statement, db.Dialect)

	if db.Conn == nil {
		return nil, errors.New("no connection")
	}
	var rows *sql.Rows
	if b.ctx != nil {
		rows, err = db.Conn.QueryContext(b.ctx, statement, args...)
	} else {
		rows, err = db.Conn.Query(statement, args...)
	}
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("queryM: no data found")
	} else if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	models := make([]interface{}, len(columns))
	modelsPtrs := make([]interface{}, len(columns))

	listMap := make([]map[string]interface{}, 0)

	for rows.Next() {
		for i := range models {
			models[i] = &modelsPtrs[i]
		}

		err := rows.Scan(models...)
		if err != nil {
			return nil, err
		}

		m := map[string]interface{}{}
		for i := range columns {
			if v, ok := modelsPtrs[i].([]byte); ok {
				modelsPtrs[i] = string(v)
			}
			m[columns[i]] = modelsPtrs[i]
		}
		listMap = append(listMap, m)
	}
	if len(listMap) == 0 {
		return nil, errors.New("no data found")
	}
	return listMap, nil
}

func Query(dbName string, statement string, args ...any) ([]map[string]interface{}, error) {
	if dbName == "" {
		dbName = databases[0].Name
	}
	db, err := GetMemoryDatabase(dbName)
	if err != nil {
		return nil, err
	}
	adaptPlaceholdersToDialect(&statement, db.Dialect)

	var rows *sql.Rows
	rows, err = db.Conn.Query(statement, args...)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("queryM: no data found")
	} else if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	models := make([]interface{}, len(columns))
	modelsPtrs := make([]interface{}, len(columns))

	listMap := make([]map[string]interface{}, 0)

	for rows.Next() {
		for i := range models {
			models[i] = &modelsPtrs[i]
		}

		err := rows.Scan(models...)
		if err != nil {
			return nil, err
		}

		m := map[string]interface{}{}
		for i := range columns {
			if v, ok := modelsPtrs[i].([]byte); ok {
				modelsPtrs[i] = string(v)
			}
			m[columns[i]] = modelsPtrs[i]
		}
		listMap = append(listMap, m)
	}
	if len(listMap) == 0 {
		return nil, errors.New("no data found")
	}
	return listMap, nil
}

func ExecSQL(dbName, query string, args ...any) error {
	conn,ok := GetConnection(dbName)
	if !ok {
		return errors.New("no connection found")
	}
	_, err := conn.Exec(query, args...)
	if err != nil {
		return err
	}
	return nil
}
