package korm

import (
	"database/sql"
)

var (
	MaxDbTraces = 50
)

const (
	MIGRATION_FOLDER         = "migrations"
	SQLITE           Dialect = "sqlite3"
	POSTGRES         Dialect = "postgres"
	MYSQL            Dialect = "mysql"
	MARIA            Dialect = "maria"
	COCKROACH        Dialect = "cockroach"
)

// Dialect db dialects are SQLITE, POSTGRES, MYSQL, MARIA, COCKROACH
type Dialect = string

// DatabaseEntity hold table state
type TableEntity struct {
	Types      map[string]string
	ModelTypes map[string]string
	Tags       map[string][]string
	Columns    []string
	Fkeys      []kormFkey
	Pk         string
	Name       string
}

type TablesInfos struct {
	Id         uint
	Name       string
	Pk         string
	Types      map[string]string
	ModelTypes map[string]string
	Tags       map[string][]string
	Columns    []string
	Fkeys      []string
}

// DatabaseEntity hold memory db state
type DatabaseEntity struct {
	Tables  []TableEntity
	Name    string
	Dialect string
	Conn    *sql.DB
}

type dbCache struct {
	limit      int
	page       int
	database   string
	table      string
	selected   string
	orderBys   string
	whereQuery string
	offset     string
	statement  string
	args       string
}

type DocsSuccess struct {
	Success string `json:"success" example:"success message"`
}

type DocsError struct {
	Error string `json:"error" example:"error message"`
}
