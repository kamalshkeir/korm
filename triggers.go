package korm

import (
	"strings"

	"github.com/kamalshkeir/klog"
)

// AddTrigger add trigger tablename_trig if col empty and tablename_trig_col if not
func AddTrigger(onTable, col, bf_af_UpdateInsertDelete string, stmt string, dbName ...string) {
	stat := []string{}
	if len(dbName) == 0 {
		dbName = append(dbName, databases[0].Name)
	}
	var dialect = ""
	db, err := GetMemoryDatabase(dbName[0])
	if !klog.CheckError(err) {
		dialect = db.Dialect
	}
	switch dialect {
	case "sqlite", "sqlite3", "":
		st := "CREATE TRIGGER IF NOT EXISTS " + onTable + "_trig"
		if col != "" {
			st += "_" + col
		}
		st += " " + bf_af_UpdateInsertDelete
		if col != "" {
			st += " OF " + col
		}
		st += " ON " + onTable
		st += " BEGIN " + stmt + ";End;"
		stat = append(stat, st)
	case POSTGRES, "cockroach", "pg", "cockroachdb":
		name := onTable + "_trig"
		if col != "" {
			name += "_" + col
		}
		st := "CREATE OR REPLACE FUNCTION " + name + "_func() RETURNS trigger AS $$"
		st += " BEGIN " + stmt + ";RETURN NEW;"
		st += "END;$$ LANGUAGE plpgsql;"
		stat = append(stat, st)
		trigCreate := "CREATE OR REPLACE TRIGGER " + name
		trigCreate += " " + bf_af_UpdateInsertDelete
		if col != "" {
			trigCreate += " OF" + col
		}
		trigCreate += " ON public." + onTable
		trigCreate += " FOR EACH ROW EXECUTE PROCEDURE " + name + "_func();"
		stat = append(stat, trigCreate)
	case MYSQL, MARIA:
		stat = append(stat, "DROP TRIGGER IF EXISTS "+onTable+"_trig_"+col+";")
		stat = append(stat, "DROP TRIGGER IF EXISTS "+onTable+"_trig;")
		st := "CREATE TRIGGER " + onTable + "_trig"
		if col != "" {
			st += "_" + col
		}
		st += " " + bf_af_UpdateInsertDelete + " ON " + onTable + " FOR EACH ROW BEGIN " + stmt
		if !strings.HasSuffix(stmt, ";") {
			st += ";"
		}
		st += "END;"
		stat = append(stat, st)
	default:
		return
	}

	if Debug {
		klog.Printf("statement: %s \n", stat)
	}

	for _, s := range stat {
		err := Exec(dbName[0], s)
		if err != nil {
			if !strings.Contains(err.Error(), "Trigger does not exist") {
				klog.Printf("rdcould not add trigger %v\n", err)
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
		klog.Printf("yl%s\n", stat)
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
		klog.Printf("rderr:%v\n", err)
	}
}

type sizeDb struct {
	Size float64
}

func StorageSize(dbName string) float64 {
	db, err := GetMemoryDatabase(dbName)
	if klog.CheckError(err) {
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

	m, err := QueryS[sizeDb](db.Name, statement)
	if klog.CheckError(err) {
		return -1
	}
	if len(m) > 0 {
		return m[0].Size / (1024 * 1024)
	}
	return -1
}
