package korm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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

var usedDB DatabaseEntity

const helpS string = `Commands :  
[databases, use, tables, columns, migrate, createsuperuser, createuser, query, getall, get, drop, delete, clear/cls, q/quit/exit, help/commands]
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

  'createsuperuser': (only with dashboard)
	  create a admin user
  
  'createuser': (only with dashboard)
	  create a regular user

  'query': 
	  query data from database 
	  (accept but not required extra param like : 'query' or 'query select * from users where ...')


  'getall': 
	  get all rows given a table name
	  (accept but not required extra param like : 'getall' or 'getall users')

  'get':
	  get single row 
	  (accept but not required extra param like : 'get' or 'get users email like "%anything%"')

  'delete':
	  delete rows where field equal_to
	  (accept but not required extra param like : 'delete' or 'delete users email="email@example.com"')

  'drop':
	  drop a table given table name
	  (accept but not required extra param like : 'drop' or 'drop users')

  'clear / cls':
	  clear shell console

  'q / quit / exit / q!':
	  exit shell

  'help':
	  show this help message
`

const commandsS string = "Commands :  [databases, use, tables, columns, migrate, query, getall, get, drop, delete, createsuperuser, createuser, clear/cls, q/q!/quit/exit, help/commands]"

// InitShell init the shell and return true if used to stop main
func InitShell() bool {
	args := os.Args
	if len(args) < 2 {
		return false
	}
	args = args[1:]
	switch args[0] {
	case "commands":
		fmt.Printf(yellow, "Usage: go run main.go shell")
		fmt.Printf(yellow, commandsS)
		return true
	case "help":
		fmt.Printf(yellow, "Usage: go run main.go shell")
		fmt.Printf(yellow, helpS)
		return true
	case "shell":
		databases := GetMemoryDatabases()
		usedDB = databases[0]
		defer usedDB.Conn.Close()
		fmt.Printf(yellow, commandsS)
		for {
			command, err := kinput.String(kinput.Blue, "> ")
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Printf(blue, "shell shutting down")
				}
				return true
			}
			spCommand := strings.Split(command, " ")
			if len(spCommand) > 1 {
				command = spCommand[0]
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
				var path string
				if len(spCommand) > 1 {
					path = spCommand[1]
				} else {
					path = kinput.Input(kinput.Blue, "path to sql file: ")
				}
				err := migratefromfile(path)
				if !klog.CheckError(err) {
					fmt.Printf(green, "migrated successfully")
				}
			case "databases":
				fmt.Printf(green, GetMemoryDatabases())
			case "use":
				var dbName string
				if len(spCommand) > 1 {
					dbName = spCommand[1]
				} else {
					dbName = kinput.Input(kinput.Blue, "database name: ")
				}
				db, err := GetMemoryDatabase(dbName)
				if err != nil {
					klog.Printfs("rd%v\n", err)
				}
				usedDB = *db
				fmt.Printf(green, "you are using database "+usedDB.Name)
			case "tables":
				fmt.Printf(green, GetAllTables(usedDB.Name))
			case "columns":
				var tb string
				if len(spCommand) > 1 {
					tb = spCommand[1]
				} else {
					tb = kinput.Input(kinput.Blue, "Table name: ")
				}
				if tb == "" {
					fmt.Printf(red, "you should specify a table that exist !")
				}
				mcols := GetAllColumnsTypes(tb, usedDB.Name)
				cols := []string{}
				for k := range mcols {
					cols = append(cols, k)
				}
				fmt.Printf(green, cols)
			case "getall":
				if len(spCommand) > 1 {
					getAll(spCommand[1])
				} else {
					getAll("")
				}
			case "get":
				if len(spCommand) > 2 {
					getRow(spCommand[1], strings.Join(spCommand[2:], " "))
				} else {
					getRow("", "")
				}
			case "query":
				if len(spCommand) > 1 {
					query(strings.Join(spCommand[1:], " "))
				} else {
					query("")
				}
			case "drop":
				if len(spCommand) > 1 {
					dropTable(spCommand[1])
				} else {
					dropTable("")
				}
			case "delete":
				if len(spCommand) > 2 {
					deleteRow(spCommand[1], strings.Join(spCommand[2:], " "))
				} else {
					deleteRow("", "")
				}
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

func getAll(tbName string) {
	if tbName == "" {
		tbName = kinput.Input(kinput.Blue, "Enter a table name: ")
	}
	data, err := Table(tbName).Database(usedDB.Name).All()
	if err == nil {
		d, _ := json.MarshalIndent(data, "", "    ")
		fmt.Printf(green, string(d))
	} else {
		fmt.Printf(red, err.Error())
	}
}

func query(queryStatement string) {
	if queryStatement == "" {
		queryStatement = kinput.Input(kinput.Blue, "Query: ")
	}
	data, err := Query(usedDB.Name, queryStatement)
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
	if !IsValidEmail(email) {
		return fmt.Errorf("email not valid")
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

func getRow(tbName, where string) {
	if tbName == "" || where == "" {
		tbName = kinput.Input(kinput.Blue, "Table Name: ")
		where = kinput.Input(kinput.Blue, "Where Query: ")
	}

	if tbName != "" && where != "" {
		var data map[string]any
		var err error
		data, err = Table(tbName).Database(usedDB.Name).Where(where).One()
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
		conn := usedDB.Conn
		_, err := conn.Exec(statements[i])
		if err != nil {
			return errors.New("error migrating from " + path + " " + err.Error())
		}
	}
	return nil
}

func dropTable(tbName string) {
	if tbName == "" {
		tbName = kinput.Input(kinput.Blue, "Table to drop : ")
	}

	_, err := Table(tbName).Database(usedDB.Name).Drop()
	if err != nil {
		fmt.Printf(red, "error dropping table :"+err.Error())
	} else {
		fmt.Printf(green, tbName+" dropped")
	}
}

func deleteRow(tbName, where string) {
	if tbName == "" || where == "" {
		tbName = kinput.Input(kinput.Blue, "Table Name: ")
		where = kinput.Input(kinput.Blue, "Where Query: ")
	}
	if tbName != "" && where != "" {
		_, err := Table(tbName).Database(usedDB.Name).Where(where).Delete()
		if err == nil {
			fmt.Printf(green, tbName+" deleted.")
		} else {
			fmt.Printf(red, "error deleting row: "+err.Error())
		}
	} else {
		fmt.Printf(red, "some of args are empty")
	}
}
