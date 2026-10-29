package korm

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// MCPGetSchemaResult encapsulates the schema and relations of a database.
type MCPGetSchemaResult struct {
	Database  string                    `json:"database"`
	Dialect   string                    `json:"dialect"`
	Tables    []TableSummary            `json:"tables"`
	Relations map[string][]TableRelation `json:"relations"`
}

// MCPGetSchema returns the full schema and relations graph for the given database (or default DB).
func MCPGetSchema(dbName ...string) (*MCPGetSchemaResult, error) {
	db := defaultDB
	if len(dbName) > 0 && dbName[0] != "" {
		db = dbName[0]
	}
	if db == "" && len(databases) > 0 {
		db = databases[0].Name
	}
	if db == "" {
		return nil, errors.New("no database connected")
	}

	dialect := ""
	if d, err := GetMemoryDatabase(db); err == nil && d != nil {
		dialect = d.Dialect
	}

	entities := schemaEntities(db)
	summaries := tableSummaries(db, entities)
	relations := buildTableRelations(entities)

	return &MCPGetSchemaResult{
		Database:  db,
		Dialect:   dialect,
		Tables:    summaries,
		Relations: relations,
	}, nil
}

// MCPExecuteReadOnlyQueryResult represents the output of a safe read-only SQL query.
type MCPExecuteReadOnlyQueryResult struct {
	Query      string           `json:"query"`
	Columns    []string         `json:"columns"`
	Rows       []map[string]any `json:"rows"`
	Count      int              `json:"count"`
	DurationMs float64          `json:"duration_ms"`
}

var (
	disallowedSQLRegex = regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|GRANT|REVOKE|ATTACH|DETACH|EXEC)\b`)
	limitRegex         = regexp.MustCompile(`(?i)\bLIMIT\s+\d+`)
)

// MCPExecuteReadOnlyQuery securely runs a SELECT-only query with enforced limit and returns map rows.
func MCPExecuteReadOnlyQuery(query string, dbName string, maxLimit ...int) (*MCPExecuteReadOnlyQueryResult, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, errors.New("empty query")
	}

	// Remove trailing semicolon if present
	q = strings.TrimSuffix(q, ";")
	q = strings.TrimSpace(q)

	// Disallow stacked queries (multiple statements separated by semicolon)
	if strings.Contains(q, ";") {
		return nil, errors.New("multiple SQL statements are not permitted for security")
	}

	// Verify query starts with SELECT, WITH, or EXPLAIN
	upper := strings.ToUpper(q)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") && !strings.HasPrefix(upper, "EXPLAIN") {
		return nil, errors.New("only read-only queries (SELECT, WITH, EXPLAIN) are permitted")
	}

	// Check for forbidden mutation keywords
	if disallowedSQLRegex.MatchString(q) {
		return nil, errors.New("query contains prohibited mutation keywords (INSERT, UPDATE, DELETE, DROP, etc.)")
	}

	// Enforce LIMIT to protect server memory
	limit := 50
	if len(maxLimit) > 0 && maxLimit[0] > 0 {
		limit = maxLimit[0]
		if limit > 200 {
			limit = 200
		}
	}
	if !limitRegex.MatchString(q) && !strings.HasPrefix(upper, "EXPLAIN") {
		q = fmt.Sprintf("%s LIMIT %d", q, limit)
	}

	db := defaultDB
	if dbName != "" {
		db = dbName
	}
	if db == "" && len(databases) > 0 {
		db = databases[0].Name
	}

	start := time.Now()
	rows, err := Select[map[string]any]().Database(db).Query(q)
	if err != nil {
		return nil, err
	}
	duration := float64(time.Since(start).Microseconds()) / 1000.0

	var cols []string
	if len(rows) > 0 {
		for k := range rows[0] {
			cols = append(cols, k)
		}
	}

	return &MCPExecuteReadOnlyQueryResult{
		Query:      q,
		Columns:    cols,
		Rows:       rows,
		Count:      len(rows),
		DurationMs: duration,
	}, nil
}

// MCPGetDiffResult wraps schema diff information.
type MCPGetDiffResult struct {
	Database     string      `json:"database"`
	InSync       bool        `json:"in_sync"`
	Diffs        []TableDiff `json:"diffs"`
	SuggestedSQL []string    `json:"suggested_sql"`
}

// MCPGetDiff returns differences between current Go models and physical DB schema.
func MCPGetDiff(dbName ...string) (*MCPGetDiffResult, error) {
	db := defaultDB
	if len(dbName) > 0 && dbName[0] != "" {
		db = dbName[0]
	}
	if db == "" && len(databases) > 0 {
		db = databases[0].Name
	}

	diffs, err := CheckSchema(db)
	if err != nil {
		return nil, err
	}

	var allSuggestedSQL []string
	inSync := true
	for _, d := range diffs {
		if len(d.AddedCols) > 0 || len(d.DroppedCols) > 0 || len(d.AlteredCols) > 0 || len(d.RenamedCols) > 0 || !d.ExistsInDB {
			inSync = false
		}
		allSuggestedSQL = append(allSuggestedSQL, d.SuggestedSQL...)
	}

	return &MCPGetDiffResult{
		Database:     db,
		InSync:       inSync,
		Diffs:        diffs,
		SuggestedSQL: allSuggestedSQL,
	}, nil
}

// MCPCacheStatsResult reports KORM memory cache metrics.
type MCPCacheStatsResult struct {
	Enabled          bool `json:"enabled"`
	ItemsCount       int  `json:"items_count"`
	MaxMemoryMb      int  `json:"max_memory_mb"`
	TrackedDatabases int  `json:"tracked_databases"`
}

// MCPGetCacheStats returns live KORM in-memory cache statistics.
func MCPGetCacheStats() *MCPCacheStatsResult {
	return &MCPCacheStatsResult{
		Enabled:          useCache,
		ItemsCount:       cacheQ.Len(),
		MaxMemoryMb:      cacheMaxMemoryMb,
		TrackedDatabases: len(databases),
	}
}

// MCPGenerateGoStruct introspects a physical or registered table and generates Go struct code with KORM tags.
func MCPGenerateGoStruct(tableName string, dbName ...string) (string, error) {
	db := defaultDB
	if len(dbName) > 0 && dbName[0] != "" {
		db = dbName[0]
	}
	if db == "" && len(databases) > 0 {
		db = databases[0].Name
	}

	entities := schemaEntities(db)
	var target *TableEntity
	for i := range entities {
		if strings.EqualFold(entities[i].Name, tableName) {
			target = &entities[i]
			break
		}
	}

	if target == nil {
		return "", fmt.Errorf("table '%s' not found in database '%s'", tableName, db)
	}

	structName := toPascalCase(target.Name)
	// Singularize basic plurals
	if strings.HasSuffix(structName, "ies") {
		structName = strings.TrimSuffix(structName, "ies") + "y"
	} else if strings.HasSuffix(structName, "s") && !strings.HasSuffix(structName, "ss") {
		structName = strings.TrimSuffix(structName, "s")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("type %s struct {\n", structName))

	for _, col := range target.Columns {
		fieldName := toPascalCase(col)
		if strings.EqualFold(col, "id") {
			fieldName = "Id"
		}

		goType := "string"
		if mt, ok := target.ModelTypes[col]; ok && mt != "" {
			goType = mt
		} else if rawType, ok := target.Types[col]; ok {
			upperRaw := strings.ToUpper(rawType)
			switch {
			case strings.Contains(upperRaw, "INT"):
				if col == target.Pk {
					goType = "uint"
				} else {
					goType = "int"
				}
			case strings.Contains(upperRaw, "BOOL"):
				goType = "bool"
			case strings.Contains(upperRaw, "FLOAT") || strings.Contains(upperRaw, "DOUBLE") || strings.Contains(upperRaw, "REAL"):
				goType = "float64"
			case strings.Contains(upperRaw, "TIME") || strings.Contains(upperRaw, "DATE"):
				goType = "time.Time"
			default:
				goType = "string"
			}
		}

		var tagParts []string
		if col == target.Pk {
			tagParts = append(tagParts, "pk")
		}

		if tags, ok := target.Tags[col]; ok {
			for _, tg := range tags {
				if tg != "pk" && tg != "" {
					tagParts = append(tagParts, tg)
				}
			}
		}

		// Look for foreign key in Fkeys
		for _, fk := range target.Fkeys {
			if fk.FromTableField == col {
				tagParts = append(tagParts, fmt.Sprintf("fk:%s", fk.ToTableField))
			}
		}

		kormTag := strings.Join(tagParts, ";")
		if kormTag != "" {
			sb.WriteString(fmt.Sprintf("\t%-14s %-12s `json:\"%s,omitempty\" korm:\"%s\"`\n", fieldName, goType, col, kormTag))
		} else {
			sb.WriteString(fmt.Sprintf("\t%-14s %-12s `json:\"%s,omitempty\"`\n", fieldName, goType, col))
		}
	}

	sb.WriteString("}\n")
	return sb.String(), nil
}

func toPascalCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	for i := range parts {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

