package korm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kamalshkeir/argon"
	"github.com/kamalshkeir/kinput"
	"github.com/kamalshkeir/ksmux"
)

const (
	red    = "\033[1;31m%v\033[0m\n"
	green  = "\033[1;32m%v\033[0m\n"
	yellow = "\033[1;33m%v\033[0m\n"
	blue   = "\033[1;34m%v\033[0m\n"
)

var usedDB DatabaseEntity

func getShellVersion() string {
	b, err := os.ReadFile("VERSION")
	if err == nil && len(b) > 0 {
		return strings.TrimSpace(string(b))
	}
	return "v1.97.93"
}

func printCommands() {
	cmdStr := "\033[0;36mtables\033[0m | \033[0;36mcolumns\033[0m | \033[0;36mdiff\033[0m | \033[0;36mexec\033[0m | \033[0;36mquery\033[0m | \033[0;36mget\033[0m | \033[0;36mgetall\033[0m | \033[0;36mdelete\033[0m | \033[0;36mdrop\033[0m | \033[0;36mdatabases\033[0m | \033[0;36muse\033[0m"
	if dahsboardUsed {
		cmdStr += " | \033[0;36mcreateuser\033[0m | \033[0;36mcreatesuperuser\033[0m"
	}
	cmdStr += " | \033[0;36mhelp\033[0m | \033[0;36mq\033[0m"
	fmt.Printf("\n  \033[1;33mCommands:\033[0m %s\n\n", cmdStr)
}

func printHelp() {
	fmt.Println()
	fmt.Println("  \033[1;36m⚡ KORM Shell Commands\033[0m")
	fmt.Println()
	fmt.Println("  \033[1;33mSchema & Migrations\033[0m")
	fmt.Println("    \033[1;32mdiff\033[0m [table] [-a]     Show schema diff (use -a / --apply to execute directly)")
	fmt.Println("    \033[1;32mtables\033[0m                List all tables in active database")
	fmt.Println("    \033[1;32mcolumns\033[0m [table]       List columns and types of a table")
	fmt.Println("    \033[1;32mexec\033[0m <file.sql>       Execute a SQL script file (alias: source, run, migrate)")
	fmt.Println()
	fmt.Println("  \033[1;33mData & Queries\033[0m")
	fmt.Println("    \033[1;32mquery\033[0m <sql>           Execute a custom SQL query")
	fmt.Println("    \033[1;32mget\033[0m <table-where>     Fetch single row (e.g. get users email = \"a@b.com\")")
	fmt.Println("    \033[1;32mgetall\033[0m [table]        Fetch all rows from a table")
	fmt.Println("    \033[1;32mdelete\033[0m <table-where>  Delete rows matching condition")
	fmt.Println("    \033[1;32mdrop\033[0m [table]          Drop a table")
	fmt.Println()
	if dahsboardUsed {
		fmt.Println("  \033[1;33mDatabase & Auth\033[0m")
		fmt.Println("    \033[1;32mdatabases\033[0m             List all connected databases")
		fmt.Println("    \033[1;32muse\033[0m <dbname>          Switch active database")
		fmt.Println("    \033[1;32mcreateuser\033[0m            Create a regular user")
		fmt.Println("    \033[1;32mcreatesuperuser\033[0m       Create an administrator user")
	} else {
		fmt.Println("  \033[1;33mDatabase\033[0m")
		fmt.Println("    \033[1;32mdatabases\033[0m             List all connected databases")
		fmt.Println("    \033[1;32muse\033[0m <dbname>          Switch active database")
	}
	fmt.Println()
	fmt.Println("  \033[1;33mGeneral\033[0m")
	fmt.Println("    \033[1;32mclear, cls\033[0m            Clear console screen")
	fmt.Println("    \033[1;32mhelp, commands\033[0m        Show this help reference")
	fmt.Println("    \033[1;32mq, exit, quit\033[0m         Exit shell")
	fmt.Println()
}

// InitShell init the shell and return true if used to stop main
func InitShell() bool {
	args := os.Args
	if len(args) < 2 {
		return false
	}
	args = args[1:]
	switch args[0] {
	case "commands":
		fmt.Printf("\033[1;33mUsage: go run main.go shell\033[0m\n")
		printCommands()
		return true
	case "help":
		fmt.Printf("\033[1;33mUsage: go run main.go shell\033[0m\n")
		printHelp()
		return true
	case "shell":
		databases := GetMemoryDatabases()
		if len(databases) == 0 {
			fmt.Println("\033[1;31m✖ No connected databases found in memory.\033[0m")
			return true
		}
		usedDB = databases[0]

		// Support direct one-shot CLI commands like 'go run main.go shell tables'
		if len(args) > 1 {
			handleCommand(args[1:])
			return true
		}
		defer usedDB.Conn.Close()

		ver := getShellVersion()
		fmt.Println()
		fmt.Printf("  \033[1;36m⚡ KORM Interactive Shell\033[0m \033[2m(%s)\033[0m\n", ver)
		fmt.Printf("  \033[0;32m● Connected:\033[0m \033[1m%s\033[0m \033[2m(%s)\033[0m\n", usedDB.Name, usedDB.Dialect)
		fmt.Printf("  \033[90mType 'help' for commands, 'q' to exit.\033[0m\n\n")

		for {
			prompt := fmt.Sprintf("\033[1;36mkorm\033[0m \033[2m(%s)\033[0m \033[1;32m❯\033[0m ", usedDB.Name)
			command, err := kinput.String("%v", prompt)
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Println("\n\033[90m  👋 Shell closed.\033[0m")
				}
				return true
			}
			command = strings.TrimSpace(command)
			if command == "" {
				continue
			}
			spCommand := strings.Fields(command)
			if handleCommand(spCommand) {
				return true
			}
		}
	case "gendocs", "gendoc":
		if len(args) > 1 && args[1] != "" {
			ksmux.DocsOutJson = args[1]
			_ = ksmux.CheckAndInstallSwagger()
			ksmux.GenerateGoDocsComments()
			ksmux.GenerateJsonDocs()
		} else {
			ksmux.DocsOutJson = "./" + staticDir + "/docs"
			_ = ksmux.CheckAndInstallSwagger()
			ksmux.GenerateGoDocsComments()
			ksmux.GenerateJsonDocs()
		}
		return true
	default:
		return false
	}
}

func handleCommand(commands []string) bool {
	command := ""
	if len(commands) > 0 {
		command = commands[0]
	}
	switch command {
	case "quit", "exit", "q", "q!":
		fmt.Println("\033[90m  👋 Goodbye!\033[0m")
		return true
	case "clear", "cls":
		kinput.Clear()
		ver := getShellVersion()
		fmt.Printf("\n  \033[1;36m⚡ KORM Shell\033[0m \033[2m(%s)\033[0m  \033[90m•\033[0m  \033[1m%s\033[0m \033[2m(%s)\033[0m\n\n", ver, usedDB.Name, usedDB.Dialect)
	case "help":
		printHelp()
	case "commands":
		printCommands()
	case "diff":
		handleDiffCommand(commands)
	case "exec", "source", "run", "migrate":
		var path string
		if len(commands) > 1 {
			path = commands[1]
		} else {
			path = kinput.Input(kinput.Blue, "Path to SQL file: ")
		}
		path = strings.TrimSpace(path)
		if path == "" {
			fmt.Println("  \033[1;31m✖ Path cannot be empty.\033[0m")
			return false
		}
		err := executeSQLFile(path)
		if err == nil {
			fmt.Printf("  \033[1;32m✔ Executed '%s' successfully.\033[0m\n", path)
		} else {
			fmt.Printf("  \033[1;31m✖ Execution error: %v\033[0m\n", err)
		}
	case "databases":
		dbs := GetMemoryDatabases()
		fmt.Printf("\n  \033[1;36mConnected Databases (%d):\033[0m\n", len(dbs))
		for _, db := range dbs {
			if db.Name == usedDB.Name {
				fmt.Printf("    \033[1;32m● %-16s\033[0m \033[2m(%s)\033[0m \033[1;32m[active]\033[0m\n", db.Name, db.Dialect)
			} else {
				fmt.Printf("    \033[0;37m○ %-16s\033[0m \033[2m(%s)\033[0m\n", db.Name, db.Dialect)
			}
		}
		fmt.Println()
	case "use":
		var dbName string
		if len(commands) > 1 {
			dbName = commands[1]
		} else {
			dbName = kinput.Input(kinput.Blue, "Database name: ")
		}
		dbName = strings.TrimSpace(dbName)
		if dbName == "" {
			fmt.Println("  \033[1;31m✖ Database name cannot be empty.\033[0m")
			return false
		}
		db, err := GetMemoryDatabase(dbName)
		if err != nil {
			fmt.Printf("  \033[1;31m✖ Database '%s' not found.\033[0m\n", dbName)
			return false
		}
		usedDB = *db
		fmt.Printf("  \033[1;32m✔ Active database set to '%s' (%s).\033[0m\n", usedDB.Name, usedDB.Dialect)
	case "tables":
		tbs := GetAllTables(usedDB.Name)
		if len(tbs) == 0 {
			fmt.Printf("\n  \033[0;33mℹ No tables found in database '%s'.\033[0m\n\n", usedDB.Name)
			return false
		}
		fmt.Printf("\n  \033[1;36mTables in '%s' (%d):\033[0m\n", usedDB.Name, len(tbs))
		for _, t := range tbs {
			if strings.HasPrefix(t, "_") {
				fmt.Printf("    \033[90m• %-20s (internal)\033[0m\n", t)
			} else {
				fmt.Printf("    \033[1;32m•\033[0m \033[1m%s\033[0m\n", t)
			}
		}
		fmt.Println()
	case "columns":
		var tb string
		if len(commands) > 1 {
			tb = commands[1]
		} else {
			tb = kinput.Input(kinput.Blue, "Table name: ")
		}
		tb = strings.TrimSpace(tb)
		if tb == "" {
			fmt.Println("  \033[1;31m✖ You must specify a table name.\033[0m")
			return false
		}
		mcols, cols := GetAllColumnsTypes(tb, usedDB.Name)
		if len(mcols) == 0 {
			fmt.Printf("  \033[1;31m✖ Table '%s' not found or has no columns in '%s'.\033[0m\n", tb, usedDB.Name)
			return false
		}
		if len(cols) == 0 {
			for k := range mcols {
				cols = append(cols, k)
			}
			sort.Strings(cols)
		}

		pkName := ""
		for _, t := range usedDB.Tables {
			if t.Name == tb {
				pkName = t.Pk
				break
			}
		}

		fmt.Printf("\n  \033[1;36mColumns for '%s' (%d):\033[0m\n", tb, len(cols))
		for _, col := range cols {
			colType := mcols[col]
			extra := ""
			if col == pkName {
				extra = " \033[1;33m[PK]\033[0m"
			}
			fmt.Printf("    \033[1;32m•\033[0m \033[1m%-18s\033[0m \033[0;36m%s\033[0m%s\n", col, colType, extra)
		}
		fmt.Println()
	case "getall":
		if len(commands) > 1 {
			getAll(commands[1])
		} else {
			getAll("")
		}
	case "get":
		if len(commands) > 2 {
			getRow(commands[1], strings.Join(commands[2:], " "))
		} else {
			getRow("", "")
		}
	case "query":
		if len(commands) > 1 {
			query(strings.Join(commands[1:], " "))
		} else {
			query("")
		}
	case "drop":
		if len(commands) > 1 {
			dropTable(commands[1])
		} else {
			dropTable("")
		}
	case "delete":
		if len(commands) > 2 {
			deleteRow(commands[1], strings.Join(commands[2:], " "))
		} else {
			deleteRow("", "")
		}
	case "createuser":
		if !dahsboardUsed {
			fmt.Println("  \033[1;31m✖ 'createuser' is only available when WithDashboard(...) is enabled.\033[0m")
			return false
		}
		createuser()
	case "createsuperuser":
		if !dahsboardUsed {
			fmt.Println("  \033[1;31m✖ 'createsuperuser' is only available when WithDashboard(...) is enabled.\033[0m")
			return false
		}
		createsuperuser()
	default:
		fmt.Printf("  \033[1;31m✖ Unknown command: '%s'. Type 'help' for available commands.\033[0m\n", command)
	}
	return false
}

func createuser() {
	fmt.Printf("\n  \033[1;36mCreate User:\033[0m\n")
	username := strings.TrimSpace(kinput.Input(kinput.Blue, "    Username : "))
	email := strings.TrimSpace(kinput.Input(kinput.Blue, "    Email    : "))
	password := strings.TrimSpace(kinput.Hidden(kinput.Blue, "    Password : "))
	if email != "" && password != "" {
		err := newuser(username, email, password, false)
		if err == nil {
			fmt.Printf("  \033[1;32m✔ User '%s' created successfully.\033[0m\n\n", email)
		} else {
			fmt.Printf("  \033[1;31m✖ Unable to create user: %v\033[0m\n\n", err)
		}
	} else {
		fmt.Printf("  \033[1;31m✖ Email or password cannot be empty.\033[0m\n\n")
	}
}

func createsuperuser() {
	fmt.Printf("\n  \033[1;36mCreate Superuser (Admin):\033[0m\n")
	username := strings.TrimSpace(kinput.Input(kinput.Blue, "    Username : "))
	email := strings.TrimSpace(kinput.Input(kinput.Blue, "    Email    : "))
	password := strings.TrimSpace(kinput.Hidden(kinput.Blue, "    Password : "))
	if email != "" && password != "" {
		err := newuser(username, email, password, true)
		if err == nil {
			fmt.Printf("  \033[1;32m✔ Superuser '%s' created successfully.\033[0m\n\n", email)
		} else {
			fmt.Printf("  \033[1;31m✖ Error creating superuser: %v\033[0m\n\n", err)
		}
	} else {
		fmt.Printf("  \033[1;31m✖ Email or password cannot be empty.\033[0m\n\n")
	}
}

func getAll(tbName string) {
	if tbName == "" {
		tbName = kinput.Input(kinput.Blue, "Enter table name: ")
	}
	tbName = strings.TrimSpace(tbName)
	if tbName == "" {
		fmt.Println("  \033[1;31m✖ Table name cannot be empty.\033[0m")
		return
	}
	data, err := Table(tbName).Database(usedDB.Name).All()
	if err != nil {
		if errors.Is(err, ErrNoData) || strings.Contains(err.Error(), "no data") {
			fmt.Printf("\n  \033[0;33mℹ Table '%s' is empty (0 rows).\033[0m\n\n", tbName)
			return
		}
		fmt.Printf("  \033[1;31m✖ Error: %v\033[0m\n", err)
		return
	}
	if len(data) == 0 {
		fmt.Printf("\n  \033[0;33mℹ Table '%s' is empty (0 rows).\033[0m\n\n", tbName)
		return
	}
	d, _ := json.MarshalIndent(data, "    ", "  ")
	fmt.Printf("\n  \033[1;36mRows in '%s' (%d):\033[0m\n", tbName, len(data))
	fmt.Printf("    \033[0;32m%s\033[0m\n\n", string(d))
}

func query(queryStatement string) {
	if queryStatement == "" {
		queryStatement = kinput.Input(kinput.Blue, "Query: ")
	}
	queryStatement = strings.TrimSpace(queryStatement)
	if queryStatement == "" {
		fmt.Println("  \033[1;31m✖ Query cannot be empty.\033[0m")
		return
	}
	data, err := BuilderMap().Database(usedDB.Name).QueryM(queryStatement)
	if err != nil {
		fmt.Printf("  \033[1;31m✖ Query error: %v\033[0m\n", err)
		return
	}
	if len(data) == 0 {
		fmt.Printf("\n  \033[0;33mℹ Query executed successfully (0 rows returned).\033[0m\n\n")
		return
	}
	d, _ := json.MarshalIndent(data, "    ", "  ")
	fmt.Printf("\n  \033[1;36mResults (%d rows):\033[0m\n", len(data))
	fmt.Printf("    \033[0;32m%s\033[0m\n\n", string(d))
}

func newuser(username, email, password string, admin bool) error {
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
	_, err = Table("users").Database(usedDB.Name).Insert(map[string]any{
		"uuid":     GenerateUUID(),
		"email":    email,
		"password": hash,
		"username": username,
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
	tbName = strings.TrimSpace(tbName)
	where = strings.TrimSpace(where)

	if tbName != "" && where != "" {
		data, err := Table(tbName).Database(usedDB.Name).Where(where).One()
		if err != nil {
			if errors.Is(err, ErrNoData) || strings.Contains(err.Error(), "no data") {
				fmt.Printf("\n  \033[0;33mℹ No record found in '%s' matching '%s'.\033[0m\n\n", tbName, where)
				return
			}
			fmt.Printf("  \033[1;31m✖ %v\033[0m\n", err)
			return
		}
		if len(data) == 0 {
			fmt.Printf("  \033[0;33mℹ No record found matching '%s'.\033[0m\n", where)
			return
		}
		d, _ := json.MarshalIndent(data, "    ", "  ")
		fmt.Printf("\n  \033[1;36mResult from '%s':\033[0m\n", tbName)
		fmt.Printf("    \033[0;32m%s\033[0m\n\n", string(d))
	} else {
		fmt.Println("  \033[1;31m✖ Table name and where condition cannot be empty.\033[0m")
	}
}

func executeSQLFile(path string) error {
	if usedDB.Conn == nil {
		return errors.New("no database connection active")
	}
	if path == "" {
		return errors.New("path cannot be empty")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", path, err)
	}

	statements := splitSQLStatements(string(b))
	if len(statements) == 0 {
		return fmt.Errorf("file '%s' is empty or contains no valid SQL statements", path)
	}

	start := time.Now()
	for i, st := range statements {
		_, err := usedDB.Conn.Exec(st)
		if err != nil {
			preview := st
			if len(preview) > 60 {
				preview = preview[:57] + "..."
			}
			return fmt.Errorf("error in statement #%d ('%s'): %w", i+1, preview, err)
		}
	}
	elapsed := time.Since(start)
	flushCache()
	fmt.Printf("  \033[0;32m↳ %d statement(s) executed in %v\033[0m\n", len(statements), elapsed.Round(time.Millisecond))
	return nil
}

func splitSQLStatements(rawSQL string) []string {
	var statements []string
	var current strings.Builder

	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false

	runes := []rune(rawSQL)
	n := len(runes)

	for i := 0; i < n; i++ {
		r := runes[i]
		next := rune(0)
		if i+1 < n {
			next = runes[i+1]
		}

		if inLineComment {
			if r == '\n' {
				inLineComment = false
				current.WriteRune(r)
			}
			continue
		}

		if inBlockComment {
			if r == '*' && next == '/' {
				inBlockComment = false
				i++
			}
			continue
		}

		if inSingleQuote {
			current.WriteRune(r)
			if r == '\'' {
				if next == '\'' {
					current.WriteRune(next)
					i++
				} else {
					inSingleQuote = false
				}
			}
			continue
		}

		if inDoubleQuote {
			current.WriteRune(r)
			if r == '"' {
				if next == '"' {
					current.WriteRune(next)
					i++
				} else {
					inDoubleQuote = false
				}
			}
			continue
		}

		// Comment starts
		if r == '-' && next == '-' {
			inLineComment = true
			i++
			continue
		}

		if r == '/' && next == '*' {
			inBlockComment = true
			i++
			continue
		}

		if r == '\'' {
			inSingleQuote = true
			current.WriteRune(r)
			continue
		}

		if r == '"' {
			inDoubleQuote = true
			current.WriteRune(r)
			continue
		}

		if r == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteRune(r)
	}

	remaining := strings.TrimSpace(current.String())
	if remaining != "" {
		statements = append(statements, remaining)
	}

	return statements
}

func dropTable(tbName string) {
	if tbName == "" {
		tbName = kinput.Input(kinput.Blue, "Table to drop: ")
	}
	tbName = strings.TrimSpace(tbName)
	if tbName == "" {
		fmt.Println("  \033[1;31m✖ Table name cannot be empty.\033[0m")
		return
	}

	_, err := Table(tbName).Database(usedDB.Name).Drop()
	if err != nil {
		fmt.Printf("  \033[1;31m✖ Error dropping table '%s': %v\033[0m\n", tbName, err)
	} else {
		fmt.Printf("  \033[1;32m✔ Table '%s' dropped successfully.\033[0m\n", tbName)
	}
}

func deleteRow(tbName, where string) {
	if tbName == "" || where == "" {
		tbName = kinput.Input(kinput.Blue, "Table Name: ")
		where = kinput.Input(kinput.Blue, "Where Query: ")
	}
	tbName = strings.TrimSpace(tbName)
	where = strings.TrimSpace(where)
	if tbName != "" && where != "" {
		n, err := Table(tbName).Database(usedDB.Name).Where(where).Delete()
		if err != nil {
			fmt.Printf("  \033[1;31m✖ Error deleting row: %v\033[0m\n", err)
		} else {
			fmt.Printf("  \033[1;32m✔ Deleted %d row(s) from '%s'.\033[0m\n", n, tbName)
		}
	} else {
		fmt.Println("  \033[1;31m✖ Table name and where condition cannot be empty.\033[0m")
	}
}
