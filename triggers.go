package korm

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/lg"
)

func init() {
	if useCache {
		go RunEvery(FlushCacheEvery, func(cancelChan chan struct{}) {
			if !useCache {
				cancelChan <- struct{}{}
			}
			flushCache()
		})
	}
}

var (
	hooks = kmap.New[string, []HookFunc]()
)

type sizeDb struct {
	Size float64
}
type TriggersQueue struct {
	Id   uint   `korm:"pk"`
	Data string `korm:"text"`
}

type HookFunc func(HookData)

type HookData struct {
	Pk        string         `json:"pk"`
	Table     string         `json:"table"`
	Operation string         `json:"operation"`
	Data      map[string]any `json:"data"`
	Old       map[string]any `json:"old"`
	New       map[string]any `json:"new"`
}

func OnInsert(fn HookFunc) {
	if v, ok := hooks.Get("insert"); ok {
		v = append(v, fn)
		go hooks.Set("insert", v)
	} else {
		hooks.Set("insert", []HookFunc{fn})
	}
}

func OnSet(fn HookFunc) {
	if v, ok := hooks.Get("update"); ok {
		v = append(v, fn)
		go hooks.Set("update", v)
	} else {
		hooks.Set("update", []HookFunc{fn})
	}
}

func OnDelete(fn HookFunc) {
	if v, ok := hooks.Get("delete"); ok {
		v = append(v, fn)
		go hooks.Set("delete", v)
	} else {
		hooks.Set("delete", []HookFunc{fn})
	}
}

func OnDrop(fn HookFunc) {
	if v, ok := hooks.Get("drop"); ok {
		v = append(v, fn)
		go hooks.Set("drop", v)
	} else {
		hooks.Set("drop", []HookFunc{fn})
	}
}

func initCacheHooks() {
	// Add hook for data changes
	OnInsert(func(hd HookData) {
		flushTableCache(hd.Table)
	})

	// Add hook for updates
	OnSet(func(hd HookData) {
		flushTableCache(hd.Table)
	})

	// Add hook for deletes
	OnDelete(func(hd HookData) {
		flushTableCache(hd.Table)
	})

	// Add hook for drops
	OnDrop(func(hd HookData) {
		flushCache()
	})
}

// AddTrigger add trigger tablename_trig if col empty and tablename_trig_col if not
func AddTrigger(onTable, col, bf_af_UpdateInsertDelete string, stmt string, dbName ...string) {
	stat := []string{}
	if len(dbName) == 0 {
		dbName = append(dbName, databases[0].Name)
	}
	var dialect = ""
	db, err := GetMemoryDatabase(dbName[0])
	if !lg.CheckError(err) {
		dialect = db.Dialect
	}
	switch dialect {
	case "sqlite", "sqlite3", "":
		// Drop existing trigger first
		dropSt := "DROP TRIGGER IF EXISTS " + onTable + "_trig"
		if strings.Contains(bf_af_UpdateInsertDelete, "INSERT") {
			dropSt += "_insert"
		} else if strings.Contains(bf_af_UpdateInsertDelete, "UPDATE") {
			dropSt += "_update"
		} else if strings.Contains(bf_af_UpdateInsertDelete, "DELETE") {
			dropSt += "_delete"
		}
		if col != "" {
			dropSt += "_" + col
		}
		stat = append(stat, dropSt)

		// Create new trigger with unique name
		st := "CREATE TRIGGER IF NOT EXISTS " + onTable + "_trig"
		if strings.Contains(bf_af_UpdateInsertDelete, "INSERT") {
			st += "_insert"
		} else if strings.Contains(bf_af_UpdateInsertDelete, "UPDATE") {
			st += "_update"
		} else if strings.Contains(bf_af_UpdateInsertDelete, "DELETE") {
			st += "_delete"
		}
		if col != "" {
			st += "_" + col
		}
		st += " " + bf_af_UpdateInsertDelete
		if col != "" {
			st += " OF " + col
		}
		st += " ON " + onTable + " FOR EACH ROW"
		st += " BEGIN " + stmt + "; END;"
		stat = append(stat, st)
	case POSTGRES, "cockroach", "pg", "cockroachdb":
		// Drop existing trigger first
		if col != "" {
			stat = append(stat, `DROP TRIGGER IF EXISTS "`+onTable+`_trig_`+col+`" ON "`+onTable+`";`)
			stat = append(stat, `DROP FUNCTION IF EXISTS "`+onTable+`_trig_`+col+`_func"();`)
		}
		stat = append(stat, `DROP TRIGGER IF EXISTS "`+onTable+`_trig_`+strings.ToLower(strings.Split(bf_af_UpdateInsertDelete, " ")[1])+`" ON "`+onTable+`";`)
		stat = append(stat, `DROP FUNCTION IF EXISTS "`+onTable+`_trig_`+strings.ToLower(strings.Split(bf_af_UpdateInsertDelete, " ")[1])+`_func"();`)

		// Create function for trigger
		name := onTable + "_trig"
		if col != "" {
			name += "_" + col
		} else {
			name += "_" + strings.ToLower(strings.Split(bf_af_UpdateInsertDelete, " ")[1])
		}
		st := `CREATE OR REPLACE FUNCTION "` + name + `_func"() RETURNS trigger AS $$ 
BEGIN 
    RAISE NOTICE 'Trigger executing for %s', TG_TABLE_NAME;
    ` + stmt + ` 
    RAISE NOTICE 'Trigger completed for %s', TG_TABLE_NAME;
    IF (TG_OP = 'DELETE') THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END; 
$$ LANGUAGE plpgsql;`
		stat = append(stat, st)

		// Create trigger
		trigCreate := `CREATE TRIGGER "` + name + `" ` + bf_af_UpdateInsertDelete + ` ON "` + onTable + `" FOR EACH ROW EXECUTE FUNCTION "` + name + `_func"();`
		stat = append(stat, trigCreate)
	case MYSQL, MARIA:
		// Drop existing triggers first
		dropTriggerName := onTable + "_trig"
		if strings.Contains(bf_af_UpdateInsertDelete, "INSERT") {
			dropTriggerName += "_insert"
		} else if strings.Contains(bf_af_UpdateInsertDelete, "UPDATE") {
			dropTriggerName += "_update"
		} else if strings.Contains(bf_af_UpdateInsertDelete, "DELETE") {
			dropTriggerName += "_delete"
		}
		if col != "" {
			dropTriggerName += "_" + col
		}
		stat = append(stat, "DROP TRIGGER IF EXISTS `"+dropTriggerName+"`;")

		// Create trigger with operation-specific name
		st := "CREATE TRIGGER `" + dropTriggerName + "` " + bf_af_UpdateInsertDelete + " ON `" + onTable + "` FOR EACH ROW BEGIN " + stmt
		if !strings.HasSuffix(stmt, ";") {
			st += ";"
		}
		st += " END;"
		stat = append(stat, st)
	default:
		return
	}

	if Debug {
		lg.InfoC("debug", "stat", stat)
	}

	for _, s := range stat {
		err := Exec(dbName[0], s)
		if err != nil {
			if !strings.Contains(err.Error(), "Trigger does not exist") {
				lg.ErrorC("could not add trigger", "err", err)
				return
			}
		}
	}
}

// DropTrigger drop trigger tablename_trig if column empty and tablename_trig_column if not
func DropTrigger(tableName, column string, dbName ...string) {
	stat := "DROP TRIGGER " + tableName + "_trig"
	if column != "" {
		stat += "_" + column
	}
	stat += ";"
	if Debug {
		lg.InfoC("debug", "stat", stat)
	}
	n := databases[0].Name
	if len(dbName) > 0 {
		n = dbName[0]
	}
	err := Exec(n, stat)
	if err != nil {
		if !strings.Contains(err.Error(), "Trigger does not exist") {
			return
		}
		lg.CheckError(err)
	}
}

func StorageSize(dbName string) float64 {
	db, err := GetMemoryDatabase(dbName)
	if lg.CheckError(err) {
		return -1
	}
	var statement string
	switch db.Dialect {
	case SQLITE:
		statement = "select (page_count * page_size) as size FROM pragma_page_count(), pragma_page_size();"
	case POSTGRES, COCKROACH:
		statement = "select pg_database_size('" + db.Name + "') as size;"
	case MYSQL, MARIA:
		statement = "select SUM(data_length + index_length) as size FROM information_schema.tables WHERE table_schema = '" + db.Name + "';"
	default:
		return -1
	}

	m, err := Model[sizeDb]().Database(db.Name).QueryS(statement)
	if lg.CheckError(err) {
		return -1
	}
	if len(m) > 0 {
		return m[0].Size / (1024 * 1024)
	}
	return -1
}

// AddChangesTrigger
func AddChangesTrigger(tableName string, dbName ...string) error {
	dName := defaultDB
	if len(dbName) > 0 {
		dName = dbName[0]
	}

	db, err := GetMemoryDatabase(dName)
	if err != nil {
		return err
	}

	var t TableEntity
	for _, tt := range db.Tables {
		if tt.Name == tableName {
			t = tt
		}
	}

	// Get table columns for constructing the change data
	cols := t.Types
	if len(cols) == 0 {
		return ErrTableNotFound
	}

	switch db.Dialect {
	case SQLITE:
		// SQLite trigger for each operation
		insertStmt := `INSERT INTO _triggers_queue(data) VALUES (json_object('operation','insert','table','` + tableName + `','data',json_object(` + buildJsonFields("NEW", cols) + `)))`
		updateStmt := `INSERT INTO _triggers_queue(data) VALUES (json_object('operation','update','table','` + tableName + `','old',json_object(` + buildJsonFields("OLD", cols) + `),'new',json_object(` + buildJsonFields("NEW", cols) + `)))`
		deleteStmt := `INSERT INTO _triggers_queue(data) VALUES (json_object('operation','delete','table','` + tableName + `','data',json_object(` + buildJsonFields("OLD", cols) + `)))`

		// Drop all existing triggers first
		Exec(dName, `DROP TRIGGER IF EXISTS `+tableName+`_trig_insert`)
		Exec(dName, `DROP TRIGGER IF EXISTS `+tableName+`_trig_update`)
		Exec(dName, `DROP TRIGGER IF EXISTS `+tableName+`_trig_delete`)

		// Create triggers for each operation with unique names
		AddTrigger(tableName, "", "AFTER INSERT", insertStmt, dName)
		AddTrigger(tableName, "", "AFTER UPDATE", updateStmt, dName)
		AddTrigger(tableName, "", "AFTER DELETE", deleteStmt, dName)

		// Start background worker to publish changes
		go func() {
			for {
				tx, err := db.Conn.Begin()
				if err != nil {
					time.Sleep(time.Second)
					continue
				}

				// Get rows with exclusive lock
				rows, err := tx.Query("SELECT rowid, data FROM _triggers_queue")
				if err != nil {
					tx.Rollback()
					time.Sleep(time.Second)
					continue
				}
				hasRows := false
				doFlush := false
				for rows.Next() {
					doFlush = true
					hasRows = true
					var jsonData string
					var rowid int64
					if err := rows.Scan(&rowid, &jsonData); err != nil {
						continue
					}

					ddd := HookData{}
					err = json.Unmarshal([]byte(jsonData), &ddd)
					if lg.CheckError(err) {
						continue
					}
					ddd.Pk = t.Pk
					// Delete processed row within transaction
					if _, err := tx.Exec("DELETE FROM _triggers_queue WHERE rowid = ?", rowid); err == nil {
						if hhh, ok := hooks.Get(ddd.Operation); ok {
							for _, h := range hhh {
								h(ddd)
							}
						}
					}
				}
				if doFlush {
					flushTableCache(tableName)
				}
				rows.Close()
				if !hasRows {
					tx.Rollback()
					time.Sleep(time.Second)
					continue
				}

				if err := tx.Commit(); err != nil {
					lg.Error("Failed to commit transaction:", err)
					tx.Rollback()
				}

				time.Sleep(time.Second)
			}
		}()

	case POSTGRES:
		// Postgres trigger for each operation
		insertStmt := `INSERT INTO "_triggers_queue"(data) VALUES (jsonb_build_object('operation', 'insert', 'table', '` + tableName + `', 'data', to_jsonb(NEW)));`
		updateStmt := `INSERT INTO "_triggers_queue"(data) VALUES (jsonb_build_object('operation', 'update', 'table', '` + tableName + `', 'old', to_jsonb(OLD), 'new', to_jsonb(NEW)));`
		deleteStmt := `INSERT INTO "_triggers_queue"(data) VALUES (jsonb_build_object('operation', 'delete', 'table', '` + tableName + `', 'data', to_jsonb(OLD)));`

		// Create triggers for each operation
		AddTrigger(tableName, "", "AFTER INSERT", insertStmt, dName)
		AddTrigger(tableName, "", "AFTER UPDATE", updateStmt, dName)
		AddTrigger(tableName, "", "AFTER DELETE", deleteStmt, dName)

		// Start background worker to publish changes
		go func() {
			for {
				// Start transaction
				tx, err := db.Conn.Begin()
				if err != nil {
					time.Sleep(time.Second)
					continue
				}

				// Get and lock a single row
				var jsonData string
				row := tx.QueryRow("SELECT data FROM \"_triggers_queue\" LIMIT 1 FOR UPDATE SKIP LOCKED")
				err = row.Scan(&jsonData)
				if err != nil {
					tx.Rollback()
					time.Sleep(time.Second)
					continue
				}

				ddd := HookData{}
				err = json.Unmarshal([]byte(jsonData), &ddd)
				if lg.CheckError(err) {
					continue
				}
				ddd.Pk = t.Pk
				// Delete the processed row
				_, err = tx.Exec("DELETE FROM \"_triggers_queue\" WHERE data = $1", jsonData)
				if err != nil {
					tx.Rollback()
					time.Sleep(time.Second)
					continue
				}

				// Commit transaction
				err = tx.Commit()
				if err != nil {
					tx.Rollback()
					time.Sleep(time.Second)
					continue
				}
				flushTableCache(tableName)
				if hhh, ok := hooks.Get(ddd.Operation); ok {
					for _, h := range hhh {
						h(ddd)
					}
				}
				time.Sleep(time.Second)
			}
		}()

	case MYSQL:
		// MySQL trigger for each operation
		insertStmt := `INSERT INTO ` + "`_triggers_queue`" + `(data) VALUES (JSON_OBJECT('operation', 'insert', 'table', '` + tableName + `', 'data', JSON_OBJECT(` + buildJsonFields("NEW", cols) + `)))`
		updateStmt := `INSERT INTO ` + "`_triggers_queue`" + `(data) VALUES (JSON_OBJECT('operation', 'update', 'table', '` + tableName + `', 'old', JSON_OBJECT(` + buildJsonFields("OLD", cols) + `), 'new', JSON_OBJECT(` + buildJsonFields("NEW", cols) + `)))`
		deleteStmt := `INSERT INTO ` + "`_triggers_queue`" + `(data) VALUES (JSON_OBJECT('operation', 'delete', 'table', '` + tableName + `', 'data', JSON_OBJECT(` + buildJsonFields("OLD", cols) + `)))`

		if Debug {
			lg.InfoC("debug mysql trigger statements",
				"insert", insertStmt,
				"update", updateStmt,
				"delete", deleteStmt)
		}

		// Create triggers for each operation
		AddTrigger(tableName, "", "AFTER INSERT", insertStmt, dName)
		AddTrigger(tableName, "", "AFTER UPDATE", updateStmt, dName)
		AddTrigger(tableName, "", "AFTER DELETE", deleteStmt, dName)

		// Start background worker to publish changes
		go func() {
			for {
				// Start transaction
				tx, err := db.Conn.Begin()
				if err != nil {
					time.Sleep(time.Second)
					continue
				}

				// Get and lock a single row
				var jsonData string
				var id int64
				row := tx.QueryRow("SELECT id, JSON_UNQUOTE(data) FROM `_triggers_queue` LIMIT 1 FOR UPDATE")
				err = row.Scan(&id, &jsonData)
				if err != nil {
					tx.Rollback()
					time.Sleep(time.Second)
					continue
				}

				// Delete the row within the transaction
				_, err = tx.Exec("DELETE FROM `_triggers_queue` WHERE id = ?", id)
				if err != nil {
					tx.Rollback()
					time.Sleep(time.Second)
					continue
				}

				// Commit transaction
				err = tx.Commit()
				if err != nil {
					tx.Rollback()
					time.Sleep(time.Second)
					continue
				}

				// Publish change only after successful commit
				ddd := HookData{}
				ddd.Pk = t.Pk
				err = json.Unmarshal([]byte(jsonData), &ddd)
				if lg.CheckError(err) {
					continue
				}
				flushTableCache(tableName)
				if hhh, ok := hooks.Get(ddd.Operation); ok {
					for _, h := range hhh {
						h(ddd)
					}
				}
				time.Sleep(time.Second)
			}
		}()
	}

	return nil
}

// Helper function to build JSON field pairs for triggers
func buildJsonFields(prefix string, cols map[string]string) string {
	pairs := make([]string, 0, len(cols))
	for col := range cols {
		pairs = append(pairs, "'"+col+"',"+prefix+"."+col)
	}
	return strings.Join(pairs, ",")
}
