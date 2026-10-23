package korm

import (
	"strings"

	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
)

// TableRelation describes how two tables are linked through a foreign key.
// It is always expressed from the point of view of "Table":
//
//   - Kind "belongs_to": Table.Field references Other.OtherField
//   - Kind "has_many":   Other.OtherField references Table.Field
type TableRelation struct {
	Table      string `json:"table"`
	Other      string `json:"other"`
	Field      string `json:"field"`
	OtherField string `json:"other_field"`
	Kind       string `json:"kind"`
	Unique     bool   `json:"unique"`
}

// Relation kinds.
const (
	RelationBelongsTo = "belongs_to"
	RelationHasMany   = "has_many"
)

// TableSummary is a lightweight description of a table used by the dashboard
// to build navigation trees and schema graphs without loading any rows.
type TableSummary struct {
	Name    string   `json:"name"`
	Count   int      `json:"count"`
	Columns []string `json:"columns"`
	Pk      string   `json:"pk"`
	System  bool     `json:"system"`
}

// TableCard is the view model rendered by the "Tables" page.
type TableCard struct {
	Name       string
	Count      int
	Columns    []string
	Pk         string
	System     bool
	Relations  []TableRelation
	RelPreview []TableRelation
	RelExtra   int
}

// buildTableCards turns the schema into the card view model used by the
// tables page: relation previews are pre-computed server side.
func buildTableCards(summaries []TableSummary, relations map[string][]TableRelation) []TableCard {
	cards := make([]TableCard, 0, len(summaries))
	const previewLimit = 3
	for _, s := range summaries {
		rels := relations[s.Name]
		if rels == nil {
			rels = []TableRelation{}
		}
		preview := rels
		if len(preview) > previewLimit {
			preview = preview[:previewLimit]
		}
		cards = append(cards, TableCard{
			Name:       s.Name,
			Count:      s.Count,
			Columns:    s.Columns,
			Pk:         s.Pk,
			System:     s.System,
			Relations:  rels,
			RelPreview: preview,
			RelExtra:   len(rels) - len(preview),
		})
	}
	return cards
}

// schemaEntities returns the table entities known for the default database.
// It merges the in-memory schema (populated by AutoMigrate/LinkModel) with the
// schema persisted in the "_tables_infos" table so relations survive restarts
// even when a model is not migrated again at boot.
func schemaEntities(dbName string) []TableEntity {
	seen := map[string]bool{}
	entities := []TableEntity{}

	dbMem, err := GetMemoryDatabase(dbName)
	if err == nil && dbMem != nil {
		for _, t := range dbMem.Tables {
			if seen[t.Name] || strings.HasPrefix(t.Name, "_") {
				continue
			}
			seen[t.Name] = true
			entities = append(entities, t)
		}
	}

	for _, t := range GetTablesInfosFromDB(dbName) {
		if seen[t.Name] || strings.HasPrefix(t.Name, "_") {
			continue
		}
		seen[t.Name] = true
		entities = append(entities, t)
	}

	return entities
}

// buildTableRelations computes the relation graph of the given schema.
// The result maps a table name to every relation it participates in, in both
// directions (outgoing "belongs_to" and incoming "has_many").
func buildTableRelations(entities []TableEntity) map[string][]TableRelation {
	relations := map[string][]TableRelation{}
	known := map[string]bool{}
	for _, t := range entities {
		known[t.Name] = true
	}

	for _, t := range entities {
		for _, fk := range t.Fkeys {
			from := strings.SplitN(fk.FromTableField, ".", 2)
			to := strings.SplitN(fk.ToTableField, ".", 2)
			if len(from) != 2 || len(to) != 2 {
				continue
			}
			src, srcField := from[0], from[1]
			dst, dstField := to[0], to[1]
			if src == "" || dst == "" {
				continue
			}

			// src.srcField -> dst.dstField
			relations[src] = append(relations[src], TableRelation{
				Table:      src,
				Other:      dst,
				Field:      srcField,
				OtherField: dstField,
				Kind:       RelationBelongsTo,
				Unique:     fk.Unique,
			})

			// Reverse direction, seen from the referenced table.
			relations[dst] = append(relations[dst], TableRelation{
				Table:      dst,
				Other:      src,
				Field:      dstField,
				OtherField: srcField,
				Kind:       RelationHasMany,
				Unique:     fk.Unique,
			})
		}
	}

	// Drop relations pointing to tables that are not part of the schema.
	for name, list := range relations {
		filtered := list[:0]
		for _, r := range list {
			if r.Other != name && (known[r.Other] || r.Other != "") {
				filtered = append(filtered, r)
			}
		}
		relations[name] = filtered
	}

	return relations
}

// tableSummaries returns the list of tables with their row count, column list
// and primary key. Row counts are fetched in a single UNION ALL query.
func tableSummaries(dbName string, entities []TableEntity) []TableSummary {
	counts := map[string]int{}
	names := []string{}
	for _, t := range entities {
		names = append(names, t.Name)
	}

	if len(names) > 0 {
		parts := make([]string, 0, len(names))
		for _, n := range names {
			parts = append(parts, "SELECT '"+n+"' AS table_name, COUNT(*) AS count FROM "+n)
		}
		rows, err := Select[tableResult]().Database(dbName).Query(strings.Join(parts, " UNION ALL "))
		if err == nil {
			for _, r := range rows {
				counts[r.TableName] = r.Count
			}
		} else {
			lg.ErrorC("unable to count tables", "err", err)
		}
	}

	summaries := make([]TableSummary, 0, len(entities))
	for _, t := range entities {
		cols := t.Columns
		if len(cols) == 0 {
			if _, c := GetAllColumnsTypes(t.Name, dbName); c != nil {
				cols = c
			}
		}
		pk := t.Pk
		if pk == "" {
			pk = "id"
		}
		summaries = append(summaries, TableSummary{
			Name:    t.Name,
			Count:   counts[t.Name],
			Columns: cols,
			Pk:      pk,
			System:  t.Name == "users" || strings.HasPrefix(t.Name, "_"),
		})
	}
	return summaries
}

// TablesListView returns a JSON summary of every table of the default database.
// It powers the sidebar table tree and the command palette.
var TablesListView = func(c *ksmux.Context) {
	entities := schemaEntities(defaultDB)
	c.Json(map[string]any{
		"success": tableSummaries(defaultDB, entities),
	})
}

// TableRelationsView returns the JSON relation graph of the default database.
var TableRelationsView = func(c *ksmux.Context) {
	entities := schemaEntities(defaultDB)
	c.Json(map[string]any{
		"success": buildTableRelations(entities),
	})
}

// SchemaGraphView returns the JSON payload used by the dashboard schema map:
// tables, relations and row counts.
var SchemaGraphView = func(c *ksmux.Context) {
	entities := schemaEntities(defaultDB)
	c.Json(map[string]any{
		"success": map[string]any{
			"tables":    tableSummaries(defaultDB, entities),
			"relations": buildTableRelations(entities),
		},
	})
}
