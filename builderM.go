package korm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kstrct"
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
		klog.Printf("rdUse korm.Table before .Where\n")
		return nil
	}
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

func (b *BuilderM) Insert(rowData map[string]any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find table, try db.Table before")
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
	if err != nil {
		return 0, err
	}

	placeholdersSlice := []string{}
	keys := []string{}
	values := []any{}
	count := 0
	for k, v := range rowData {
		switch db.Dialect {
		case POSTGRES, SQLITE:
			placeholdersSlice = append(placeholdersSlice, "$"+strconv.Itoa(count+1))
		case MYSQL, MARIA, "mariadb":
			placeholdersSlice = append(placeholdersSlice, "?")
		default:
			return 0, errors.New("database is neither sqlite, postgres or mysql")
		}
		keys = append(keys, k)
		values = append(values, v)
		count++
	}
	placeholders := strings.Join(placeholdersSlice, ",")
	var affectedRows int

	stat := strings.Builder{}
	stat.WriteString("INSERT INTO " + b.tableName + " (")
	stat.WriteString(strings.Join(keys, ","))
	stat.WriteString(") VALUES (")
	stat.WriteString(placeholders)
	stat.WriteString(")")
	statement := stat.String()
	var res sql.Result
	if b.ctx != nil {
		res, err = db.Conn.ExecContext(b.ctx, statement, values...)
	} else {
		res, err = db.Conn.Exec(statement, values...)
	}
	if err != nil {
		if b.debug {
			klog.Printf("ylstatement: %s\nvalues: %v \n", statement, values)
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
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "update",
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
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "delete",
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
			"type": "drop",
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
		return nil, fmt.Errorf("no data found")
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

func Exec(dbName, query string, args ...any) error {
	conn, ok := GetConnection(dbName)
	if !ok {
		return errors.New("no connection found")
	}
	_, err := conn.Exec(query, args...)
	if err != nil {
		return err
	}
	return nil
}

func (b *BuilderM) AddRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try korm.AutoMigrate before")
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

func (b *BuilderM) GetRelated(relatedTable string, dest *[]map[string]any) error {
	if b.tableName == "" {
		return errors.New("unable to find model, try db.Table before")
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
	var err error
	*dest, err = Table(relationTableName).queryM(b.statement, b.args...)
	if err != nil {
		return err
	}

	return nil
}

func (b *BuilderM) JoinRelated(relatedTable string, dest *[]map[string]any) error {
	if b.tableName == "" {
		return errors.New("unable to find model, try db.Table before")
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
	var err error
	*dest, err = Table(relationTableName).queryM(b.statement, b.args...)
	if err != nil {
		return err
	}

	return nil
}

func (b *BuilderM) DeleteRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try db.Table before")
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

func (b *BuilderM) queryS(strct any, statement string, args ...any) error {
	if b.database == "" {
		b.database = databases[0].Name
	}
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
		return err
	}
	adaptPlaceholdersToDialect(&statement, db.Dialect)

	if db.Conn == nil {
		return errors.New("no connection")
	}
	var rows *sql.Rows
	if b.ctx != nil {
		rows, err = db.Conn.QueryContext(b.ctx, statement, args...)
	} else {
		rows, err = db.Conn.Query(statement, args...)
	}
	if err == sql.ErrNoRows {
		return fmt.Errorf("no data found")
	} else if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	models := make([]interface{}, len(columns))
	modelsPtrs := make([]interface{}, len(columns))

	var value = reflect.ValueOf(strct)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	} else {
		return errors.New("expected destination struct to be a pointer")
	}

	if value.Kind() != reflect.Slice {
		return fmt.Errorf("expected strct to be a ptr slice")
	}

	for rows.Next() {
		for i := range models {
			models[i] = &modelsPtrs[i]
		}

		err := rows.Scan(models...)
		if err != nil {
			return err
		}

		m := map[string]interface{}{}
		for i := range columns {
			if v, ok := modelsPtrs[i].([]byte); ok {
				modelsPtrs[i] = string(v)
			}
			m[columns[i]] = modelsPtrs[i]
		}
		ptr := reflect.New(value.Type().Elem()).Interface()
		err = kstrct.FillFromMap(ptr, m)
		if err != nil {
			return err
		}
		if value.CanAddr() && value.CanSet() {
			value.Set(reflect.Append(value, reflect.ValueOf(ptr).Elem()))
		}
	}
	return nil
}
