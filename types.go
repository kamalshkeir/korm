package korm

import (
	"database/sql"
)

const (
	MIGRATION_FOLDER         = "migrations"
	CACHE_TOPIC              = "internal-db-cache"
	SQLITE           Dialect = "sqlite"
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
	Pk         string
	Name       string
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
