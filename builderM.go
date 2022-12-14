package korm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kstrct"
)

var setReplacer = strings.NewReplacer("=", "", "?", "")

// BuilderM is query builder map string any
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

// Table is a starter for BuiderM
func Table(tableName string) *BuilderM {
	return &BuilderM{
		tableName: tableName,
		database:  databases[0].Name,
	}
}

// Database allow to choose database to execute query on
func (b *BuilderM) Database(dbName string) *BuilderM {
	b.database = dbName
	return b
}

// Select select table columns to return
func (b *BuilderM) Select(columns ...string) *BuilderM {
	b.selected = strings.Join(columns, ",")
	b.order = append(b.order, "select")
	return b
}

// Where can be like: Where("id > ?",1) or Where("id",1) = Where("id = ?",1)
func (b *BuilderM) Where(query string, args ...any) *BuilderM {
	adaptWhereQuery(&query, b.tableName)
	adaptTrueFalseArgs(&args)
	b.whereQuery = query
	b.args = append(b.args, args...)
	b.order = append(b.order, "where")
	return b
}

// Query can be used like: Query("select * from table") or Query("select * from table where col like '?'","%something%")
func (b *BuilderM) Query(query string, args ...any) *BuilderM {
	b.query = query
	adaptTrueFalseArgs(&args)
	b.args = append(b.args, args...)
	b.order = append(b.order, "query")
	return b
}

// Limit set limit
func (b *BuilderM) Limit(limit int) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Limit\n")
		return nil
	}
	b.limit = limit
	b.order = append(b.order, "limit")
	return b
}

// Page return paginated elements using Limit for specific page
func (b *BuilderM) Page(pageNumber int) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Page\n")
		return nil
	}
	b.page = pageNumber
	b.order = append(b.order, "page")
	return b
}

// OrderBy can be used like: OrderBy("-id","-email") OrderBy("id","-email") OrderBy("+id","email")
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

// Context allow to query or execute using ctx
func (b *BuilderM) Context(ctx context.Context) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Context\n")
		return nil
	}
	b.ctx = ctx
	return b
}

// Debug print prepared statement and values for this operation
func (b *BuilderM) Debug() *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Debug\n")
		return nil
	}
	b.debug = true
	return b
}

// All get all data
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
		args:       fmt.Sprint(b.args...),
	}
	if useCache {
		if v, ok := cacheAllM.Get(c); ok {
			return v, nil
		}
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
		cacheAllM.Set(c, models)
	}
	return models, nil
}

// One get single row
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
		args:       fmt.Sprint(b.args...),
	}
	if useCache {
		if v, ok := cachesOneM.Get(c); ok {
			return v, nil
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
	} else {
		b.statement += " LIMIT 1"
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

// Insert add row to a table using input map, and return PK of the inserted row
func (b *BuilderM) Insert(rowData map[string]any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find table, try db.Table before")
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "create",
		})
	}
	if onInsert != nil {
		err := onInsert(b.database, b.tableName, rowData)
		if err != nil {
			return 0, err
		}

	}
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
		return 0, err
	}
	pk := ""
	var tbmem TableEntity
	for _, t := range db.Tables {
		if t.Name == b.tableName {
			pk = t.Pk
			tbmem = t
		}
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
		if v == true {
			v = 1
		} else if v == false {
			v = 0
		}

		if vvv, ok := tbmem.ModelTypes[k]; ok && strings.HasSuffix(vvv, "Time") {
			switch tyV := v.(type) {
			case time.Time:
				v = tyV.Format("2006-01-02 15:04:05")
			case string:
				v = strings.ReplaceAll(tyV, "T", " ")
			}
		}

		values = append(values, v)
		count++
	}
	placeholders := strings.Join(placeholdersSlice, ",")
	stat := strings.Builder{}
	stat.WriteString("INSERT INTO " + b.tableName + " (")
	stat.WriteString(strings.Join(keys, ","))
	stat.WriteString(") VALUES (")
	stat.WriteString(placeholders)
	stat.WriteString(")")
	statement := stat.String()
	if b.debug {
		klog.Printf("statement : %s, values : %v\n", statement, values)
	}
	var id int
	if db.Dialect != POSTGRES {
		var res sql.Result
		if b.ctx != nil {
			res, err = db.Conn.ExecContext(b.ctx, statement, values...)
		} else {
			res, err = db.Conn.Exec(statement, values...)
		}
		if err != nil {
			return 0, err
		}
		rows, err := res.LastInsertId()
		if err != nil {
			id = -1
		} else {
			id = int(rows)
		}
	} else {
		if b.ctx != nil {
			err = db.Conn.QueryRowContext(b.ctx, statement+"RETURNING "+pk, values...).Scan(&id)
		} else {
			err = db.Conn.QueryRow(statement+"RETURNING "+pk, values...).Scan(&id)
		}
		if err != nil {
			id = -1
		}
	}
	return id, nil
}

// InsertR add row to a table using input map, and return the inserted row
func (b *BuilderM) InsertR(rowData map[string]any) (map[string]any, error) {
	if b.tableName == "" {
		return nil, errors.New("unable to find table, try db.Table before")
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "create",
		})
	}
	if onInsert != nil {
		err := onInsert(b.database, b.tableName, rowData)
		if err != nil {
			return nil, err
		}

	}
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
		return nil, err
	}
	pk := ""
	var tbmem TableEntity
	for _, t := range db.Tables {
		if t.Name == b.tableName {
			pk = t.Pk
			tbmem = t
		}
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
			return nil, errors.New("database is neither sqlite, postgres or mysql")
		}
		keys = append(keys, k)
		if v == true {
			v = 1
		} else if v == false {
			v = 0
		}

		if vvv, ok := tbmem.ModelTypes[k]; ok && strings.HasSuffix(vvv, "Time") {
			switch tyV := v.(type) {
			case time.Time:
				v = tyV.Format("2006-01-02 15:04:05")
			case string:
				v = strings.ReplaceAll(tyV, "T", " ")
			}
		}

		values = append(values, v)
		count++
	}
	placeholders := strings.Join(placeholdersSlice, ",")
	stat := strings.Builder{}
	stat.WriteString("INSERT INTO " + b.tableName + " (")
	stat.WriteString(strings.Join(keys, ","))
	stat.WriteString(") VALUES (")
	stat.WriteString(placeholders)
	stat.WriteString(")")
	statement := stat.String()
	if b.debug {
		klog.Printf("statement : %s, values : %v\n", statement, values)
	}
	var id int
	if db.Dialect != POSTGRES {
		var res sql.Result
		if b.ctx != nil {
			res, err = db.Conn.ExecContext(b.ctx, statement, values...)
		} else {
			res, err = db.Conn.Exec(statement, values...)
		}
		if err != nil {
			return nil, err
		}
		rows, err := res.LastInsertId()
		if err != nil {
			id = -1
		} else {
			id = int(rows)
		}
	} else {
		if b.ctx != nil {
			err = db.Conn.QueryRowContext(b.ctx, statement+"RETURNING "+pk, values...).Scan(&id)
		} else {
			err = db.Conn.QueryRow(statement+"RETURNING "+pk, values...).Scan(&id)
		}
		if err != nil {
			return nil, err
		}
	}
	m, err := Table(b.tableName).Where(pk+"= ?", id).One()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// BulkInsert insert many row at the same time in one query
func (b *BuilderM) BulkInsert(rowsData ...map[string]any) ([]int, error) {
	if b.tableName == "" {
		return nil, errors.New("unable to find table, try db.Table before")
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
	var tbmem TableEntity
	for _, t := range db.Tables {
		if t.Name == b.tableName {
			pk = t.Pk
			tbmem = t
		}
	}
	for ii := range rowsData {
		if onInsert != nil {
			err := onInsert(b.database, b.tableName, rowsData[ii])
			if err != nil {
				return nil, err
			}
		}
		placeholdersSlice := []string{}
		keys := []string{}
		values := []any{}
		count := 0
		for k, v := range rowsData[ii] {
			switch db.Dialect {
			case POSTGRES, SQLITE:
				placeholdersSlice = append(placeholdersSlice, "$"+strconv.Itoa(count+1))
			case MYSQL, MARIA, "mariadb":
				placeholdersSlice = append(placeholdersSlice, "?")
			default:
				return nil, errors.New("database is neither sqlite, postgres or mysql")
			}
			keys = append(keys, k)
			if v == true {
				v = 1
			} else if v == false {
				v = 0
			}
			if vvv, ok := tbmem.ModelTypes[k]; ok && strings.HasSuffix(vvv, "Time") {
				switch tyV := v.(type) {
				case time.Time:
					v = tyV.Format("2006-01-02 15:04:05")
				case string:
					v = strings.ReplaceAll(tyV, "T", " ")
				}
			}
			values = append(values, v)
			count++
		}
		placeholders := strings.Join(placeholdersSlice, ",")

		stat := strings.Builder{}
		stat.WriteString("INSERT INTO " + b.tableName + " (")
		stat.WriteString(strings.Join(keys, ","))
		stat.WriteString(") VALUES (")
		stat.WriteString(placeholders)
		stat.WriteString(")")
		statement := stat.String()
		if b.debug {
			klog.Printf("%s,%s\n", statement, values)
		}
		if db.Dialect != POSTGRES {
			var res sql.Result
			if b.ctx != nil {
				res, err = db.Conn.ExecContext(b.ctx, statement, values...)
			} else {
				res, err = db.Conn.Exec(statement, values...)
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
				err = db.Conn.QueryRowContext(b.ctx, statement+"RETURNING "+pk, values...).Scan(&idInserted)
			} else {
				err = db.Conn.QueryRow(statement+"RETURNING "+pk, values...).Scan(&idInserted)
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

// Set used to update, Set("email,is_admin","example@mail.com",true) or Set("email = ? AND is_admin = ?","example@mail.com",true)
func (b *BuilderM) Set(query string, args ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try db.Table before")
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
	if err != nil {
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
		return 0, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(aff), nil
}

// Delete data from database, can be multiple, depending on the where, return affected rows(Not every database or database driver may support affected rows)
func (b *BuilderM) Delete() (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try korm.AutoMigrate before")
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type": "delete",
		})
	}
	if onDelete != nil {
		err := onDelete(b.database, b.tableName, b.whereQuery, b.args...)
		if err != nil {
			return 0, err
		}
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

// Drop drop table from db
func (b *BuilderM) Drop() (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try korm.LinkModel before Update")
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

// AddRelated used for many to many, and after korm.ManyToMany, to add a class to a student or a student to a class, class or student should exist in the database before adding them
func (b *BuilderM) AddRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try korm.AutoMigrate before")
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
	ids := make([]any, 4)
	adaptTrueFalseArgs(&whereRelatedArgs)
	adaptWhereQuery(&whereRelatedTable, relatedTable)
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
		klog.Printf("ylstatement:%s\n", stat)
		klog.Printf("ylargs:%v\n", ids)
	}
	err = Exec(b.database, stat, ids...)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// GetRelated used for many to many to get related classes to a student or related students to a class
func (b *BuilderM) GetRelated(relatedTable string, dest *[]map[string]any) error {
	if b.tableName == "" {
		return errors.New("unable to find model, try db.Table before")
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

// JoinRelated same as get, but it join data
func (b *BuilderM) JoinRelated(relatedTable string, dest *[]map[string]any) error {
	if b.tableName == "" {
		return errors.New("unable to find model, try db.Table before")
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

// DeleteRelated delete a relations many to many
func (b *BuilderM) DeleteRelated(relatedTable string, whereRelatedTable string, whereRelatedArgs ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try db.Table before")
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

func (b *BuilderM) queryM(statement string, args ...any) ([]map[string]interface{}, error) {
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

func (b *BuilderM) queryS(strct any, statement string, args ...any) error {
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

		m := map[string]any{}
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
