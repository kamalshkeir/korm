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

	"github.com/kamalshkeir/argon"
	"github.com/kamalshkeir/kinput"
	"github.com/kamalshkeir/klog"
)

const (
	red     = "\033[1;31m%v\033[0m\n"
	green   = "\033[1;32m%v\033[0m\n"
	yellow  = "\033[1;33m%v\033[0m\n"
	blue    = "\033[1;34m%v\033[0m\n"
	magenta = "\033[5;35m%v\033[0m\n"
)

var usedDB string

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

  'createsuperuser':
	  create a admin user
  
  'createuser':
	  create a regular user

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

const commandsS string = "Commands :  [databases, use, tables, columns, migrate, getall, get, drop, delete, createsuperuser, createuser, clear/cls, q!/quit/exit, help/commands]"

// InitShell init the shell and return true if used to stop main
func InitShell() bool {
	args := os.Args
	if len(args) < 2 {
		return false
	}

	switch args[1] {
	case "commands":
		fmt.Printf(yellow, "Shell Usage: go run main.go dbshell")
		fmt.Printf(yellow, commandsS)
		return true
	case "help":
		fmt.Printf(yellow, "Shell Usage: go run main.go dbshell")
		fmt.Printf(yellow, helpS)
		return true
	case "shell":
		databases := GetMemoryDatabases()
		var conn *sql.DB
		if len(databases) > 1 {
			fmt.Printf(yellow, "-----------------------------------")
			fmt.Printf(blue, "Found many databases:")
			for _, db := range databases {
				fmt.Printf(blue, `  - `+db.Name)
			}
			dbName := kinput.Input(kinput.Blue, "Enter Database Name to use: ")
			if dbName == "" {
				return true
			}
			conn = GetConnection(dbName)
			usedDB = dbName
		} else {
			conn = GetConnection()
			usedDB = databases[0].Name
		}
		defer conn.Close()

		fmt.Printf(yellow, commandsS)
		for {
			command, err := kinput.String(kinput.Blue, "> ")
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Printf(blue, "shell shutting down")
				}
				return true
			}

			switch command {
			case "quit", "exit", "q", "q!":
				return true
			case "clear", "cls":
				kinput.Clear()
				fmt.Printf(yellow, commandsS)
			case "help":
				fmt.Printf(yellow, helpS)
			case "commands":
				fmt.Printf(yellow, commandsS)
			case "migrate":
				path := kinput.Input(kinput.Blue, "path to sql file: ")
				err := migratefromfile(path)
				if !klog.CheckError(err) {
					fmt.Printf(green, "migrated successfully")
				}
			case "databases":
				fmt.Printf(green, GetMemoryDatabases())
			case "use":
				db := kinput.Input(kinput.Blue, "database name: ")
				usedDB = db
				fmt.Printf(green, "you are using database "+db)
			case "tables":
				fmt.Printf(green, GetAllTables(usedDB))
			case "columns":
				tb := kinput.Input(kinput.Blue, "Table name: ")
				mcols := GetAllColumnsTypes(tb, usedDB)
				cols := []string{}
				for k := range mcols {
					cols = append(cols, k)
				}
				fmt.Printf(green, cols)
			case "getall":
				getAll()
			case "get":
				getRow()
			case "drop":
				dropTable()
			case "delete":
				deleteRow()
			case "createuser":
				createuser()
			case "createsuperuser":
				createsuperuser()
			default:
				fmt.Printf(red, "command not handled, use 'help' or 'commands' to list available commands ")
			}
		}
	default:
		return false
	}
}

func createuser() {
	email := kinput.Input(kinput.Blue, "Email : ")
	password := kinput.Hidden(kinput.Blue, "Password : ")
	if email != "" && password != "" {
		err := newuser(email, password, false)
		if err == nil {
			fmt.Printf(green, "User "+email+" created successfully")
		} else {
			fmt.Printf(red, "unable to create user:"+err.Error())
		}
	} else {
		fmt.Printf(red, "email or password invalid")
	}
}

func createsuperuser() {
	email := kinput.Input(kinput.Blue, "Email: ")
	password := kinput.Hidden(kinput.Blue, "Password: ")
	err := newuser(email, password, true)
	if err == nil {
		fmt.Printf(green, "Admin "+email+" created successfully")
	} else {
		fmt.Printf(red, "error creating user :"+err.Error())
	}
}

func getAll() {
	tableName := kinput.Input(kinput.Blue, "Enter a table name: ")
	data, err := Table(tableName).Database(usedDB).All()
	if err == nil {
		d, _ := json.MarshalIndent(data, "", "    ")
		fmt.Printf(green, string(d))
	} else {
		fmt.Printf(red, err.Error())
	}
}

func newuser(email, password string, admin bool) error {
	if email == "" || password == "" {
		return fmt.Errorf("email or password empty")
	}

	hash, err := argon.Hash(password)
	if err != nil {
		return err
	}
	_, err = Table("users").Insert(map[string]any{
		"uuid":     GenerateUUID(),
		"email":    email,
		"password": hash,
		"is_admin": admin,
		"image":    "",
	})
	if err != nil {
		return err
	}
	return nil
}

func getRow() {
	tableName := kinput.Input(kinput.Blue, "Table Name : ")
	whereField := kinput.Input(kinput.Blue, "Where field : ")
	equalTo := kinput.Input(kinput.Blue, "Equal to : ")
	if tableName != "" && whereField != "" && equalTo != "" {
		var data map[string]any
		var err error
		data, err = Table(tableName).Database(usedDB).Where(whereField+" = ?", equalTo).One()
		if err == nil {
			d, _ := json.MarshalIndent(data, "", "    ")
			fmt.Printf(green, string(d))
		} else {
			fmt.Printf(red, "error: "+err.Error())
		}
	} else {
		fmt.Printf(red, "One or more field are empty")
	}
}

func migratefromfile(path string) error {
	if !SliceContains([]string{POSTGRES, SQLITE, MYSQL, MARIA}, databases[0].Dialect) {
		fmt.Printf(red, "database is neither postgres, sqlite or mysql ")
		return errors.New("database is neither postgres, sqlite or mysql ")
	}
	if path == "" {
		fmt.Printf(red, "path cannot be empty ")
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
		conn := GetConnection(usedDB)
		_, err := conn.Exec(statements[i])
		if err != nil {
			return errors.New("error migrating from " + path + " " + err.Error())
		}
	}
	return nil
}

func dropTable() {
	tableName := kinput.Input(kinput.Blue, "Table to drop : ")
	if tableName != "" {
		_, err := Table(tableName).Database(usedDB).Drop()
		if err != nil {
			fmt.Printf(red, "error dropping table :"+err.Error())
		} else {
			fmt.Printf(green, tableName+" dropped with success")
		}
	} else {
		fmt.Printf(red, "table is empty")
	}
}

func deleteRow() {
	tableName := kinput.Input(kinput.Blue, "Table Name: ")
	whereField := kinput.Input(kinput.Blue, "Where Field: ")
	equalTo := kinput.Input(kinput.Blue, "Equal to: ")
	if tableName != "" && whereField != "" && equalTo != "" {
		equal, err := strconv.Atoi(equalTo)
		if err != nil {
			_, err := Table(tableName).Database(usedDB).Where(whereField+" = ?", equalTo).Delete()
			if err == nil {
				fmt.Printf(green, tableName+" with "+whereField+" = "+equalTo+" deleted.")
			} else {
				fmt.Printf(red, "error deleting row: "+err.Error())
			}
		} else {
			_, err = Table(tableName).Where(whereField+" = ?", equal).Delete()
			if err == nil {
				fmt.Printf(green, tableName+" with "+whereField+" = "+equalTo+" deleted.")
			} else {
				fmt.Printf(red, "error deleting row: "+err.Error())
			}
		}
	} else {
		fmt.Printf(red, "some of args are empty")
	}
}
