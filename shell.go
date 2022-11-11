package korm

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/kamalshkeir/kinput"
	"github.com/kamalshkeir/klog"
)

const (
	Red     = "\033[1;31m%v\033[0m\n"
	Green   = "\033[1;32m%v\033[0m\n"
	Yellow  = "\033[1;33m%v\033[0m\n"
	Blue    = "\033[1;34m%v\033[0m\n"
	Magenta = "\033[5;35m%v\033[0m\n"
)

const helpS string = `Commands :  
[databases, use, tables, columns, migrate, createsuperuser, createuser, getall, get, drop, delete, clear/cls, q/quit/exit, help/commands]
  'databases':
	  list all connected databases

  'use':
	  use a specific database

  'tables':
	  list all tables in database

  'columns':
	  list all columns of a table

  'migrate':
	  migrate or execute sql file

  'getall':
	  get all rows given a table name

  'get':
	  get single row wher field equal_to

  'delete':
	  delete rows where field equal_to

  'drop':
	  drop a table given table name

  'clear/cls':
	  clear console
`

const commandsS string = "Commands :  [databases, use, tables, columns, migrate, getall, get, drop, delete, clear/cls, q!/quit/exit, help/commands]"

// InitShell init the shell and return true if used to stop main
func InitShell() bool {
	args := os.Args
	if len(args) < 2 {
		return false
	}

	switch args[1] {
	case "commands":
		fmt.Printf(Yellow, "Shell Usage: go run main.go dbshell")
		fmt.Printf(Yellow, commandsS)
		return true
	case "help":
		fmt.Printf(Yellow, "Shell Usage: go run main.go dbshell")
		fmt.Printf(Yellow, helpS)
		return true
	case "dbshell":
		databases := GetMemoryDatabases()
		var conn *sql.DB
		if len(databases) > 1 {
			fmt.Printf(Yellow, "-----------------------------------")
			fmt.Printf(Blue, "Found many databases:")
			for _, db := range databases {
				fmt.Printf(Blue, `  - `+db.Name)
			}
			dbName, err := kinput.String(kinput.Blue, "Enter Database Name to use: ")
			if klog.CheckError(err) {
				return true
			}
			if dbName == "" {
				return true
			}
			conn = GetSQLConnection(dbName)
		} else {
			conn = GetSQLConnection()
		}
		defer conn.Close()

		fmt.Printf(Yellow, commandsS)
		for {
			command, err := kinput.String(kinput.Blue, "> ")
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Printf(Blue, "shell shutting down")
				}
				return true
			}

			switch command {
			case "quit", "exit", "q", "q!":
				return true
			case "clear", "cls":
				kinput.Clear()
				fmt.Printf(Yellow, commandsS)
			case "help":
				fmt.Printf(Yellow, helpS)
			case "commands":
				fmt.Printf(Yellow, commandsS)
			case "migrate":
				path := kinput.Input(kinput.Blue, "path to sql file: ")
				err := migratefromfile(path)
				if !klog.CheckError(err) {
					fmt.Printf(Green, "migrated successfully")
				}
			case "databases":
				fmt.Printf(Green, GetMemoryDatabases())
			case "use":
				db := kinput.Input(kinput.Blue, "database name: ")
				fmt.Printf(Green, "you are using database "+db)
			case "tables":
				fmt.Printf(Green, GetAllTables(databases[0].Name))
			case "columns":
				tb := kinput.Input(kinput.Blue, "Table name: ")
				mcols := GetAllColumnsTypes(tb, databases[0].Name)
				cols := []string{}
				for k := range mcols {
					cols = append(cols, k)
				}
				fmt.Printf(Green, cols)
			case "getall":
				getAll()
			case "get":
				getRow()
			case "drop":
				dropTable()
			case "delete":
				deleteRow()
			default:
				fmt.Printf(Red, "command not handled, use 'help' or 'commands' to list available commands ")
			}
		}
	default:
		return false
	}
}

func getAll() {
	tableName, err := kinput.String(kinput.Blue, "Enter a table name: ")
	if err == nil {
		data, err := Table(tableName).Database(databases[0].Name).All()
		if err == nil {
			d, _ := json.MarshalIndent(data, "", "    ")
			fmt.Printf(Green, string(d))
		} else {
			fmt.Printf(Red, err.Error())
		}
	} else {
		fmt.Printf(Red, "table name invalid")
	}
}

func getRow() {
	tableName := kinput.Input(kinput.Blue, "Table Name : ")
	whereField := kinput.Input(kinput.Blue, "Where field : ")
	equalTo := kinput.Input(kinput.Blue, "Equal to : ")
	if tableName != "" && whereField != "" && equalTo != "" {
		var data map[string]interface{}
		var err error
		data, err = Table(tableName).Database(databases[0].Name).Where(whereField+" = ?", equalTo).One()
		if err == nil {
			d, _ := json.MarshalIndent(data, "", "    ")
			fmt.Printf(Green, string(d))
		} else {
			fmt.Printf(Red, "error: "+err.Error())
		}
	} else {
		fmt.Printf(Red, "One or more field are empty")
	}
}

func migratefromfile(path string) error {
	if !SliceContains([]string{POSTGRES, SQLITE, MYSQL, MARIA}, databases[0].Dialect) {
		fmt.Printf(Red, "database is neither postgres, sqlite or mysql ")
		return errors.New("database is neither postgres, sqlite or mysql ")
	}
	if path == "" {
		fmt.Printf(Red, "path cannot be empty ")
		return errors.New("path cannot be empty ")
	}
	statements := []string{}
	b, err := os.ReadFile(path)
	if err != nil {
		return errors.New("error reading from " + path + " " + err.Error())
	}
	splited := strings.Split(string(b), ";")
	statements = append(statements, splited...)

	//exec migrations
	for i := range statements {
		_, err := GetSQLConnection().Exec(statements[i])
		if err != nil {
			return errors.New("error migrating from " + path + " " + err.Error())
		}
	}
	return nil
}

func dropTable() {
	tableName := kinput.Input(kinput.Blue, "Table to drop : ")
	if tableName != "" {
		_, err := Table(tableName).Database(databases[0].Name).Drop()
		if err != nil {
			fmt.Printf(Red, "error dropping table :"+err.Error())
		} else {
			fmt.Printf(Green, tableName+" dropped with success")
		}
	} else {
		fmt.Printf(Red, "table is empty")
	}
}

func deleteRow() {
	tableName := kinput.Input(kinput.Blue, "Table Name: ")
	whereField := kinput.Input(kinput.Blue, "Where Field: ")
	equalTo := kinput.Input(kinput.Blue, "Equal to: ")
	if tableName != "" && whereField != "" && equalTo != "" {
		equal, err := strconv.Atoi(equalTo)
		if err != nil {
			_, err := Table(tableName).Database(databases[0].Name).Where(whereField+" = ?", equalTo).Delete()
			if err == nil {
				fmt.Printf(Green, tableName+"with"+whereField+"="+equalTo+"deleted.")
			} else {
				fmt.Printf(Red, "error deleting row: "+err.Error())
			}
		} else {
			_, err = Table(tableName).Where(whereField+" = ?", equal).Delete()
			if err == nil {
				fmt.Printf(Green, tableName+" with "+whereField+" = "+equalTo+" deleted.")
			} else {
				fmt.Printf(Red, "error deleting row: "+err.Error())
			}
		}
	} else {
		fmt.Printf(Red, "some of args are empty")
	}
}
