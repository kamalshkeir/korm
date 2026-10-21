package korm

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"

	"github.com/kamalshkeir/kinput"
	"github.com/kamalshkeir/kstrct"
	"github.com/kamalshkeir/lg"
)

// ModelColumn represents the expected definition of a column derived from a Go struct field.
type ModelColumn struct {
	Name        string
	GoType      string
	SQLType     string
	Tags        []string
	IsPK        bool
	IsAutoInc   bool
	IsNotNull   bool
	IsUnique    bool
	IsIUnique   bool // case-insensitive unique
	DefaultVal  string
	Size        string
	CheckExpr   string
	GeneratedAs string
	IsFK        bool
	FKTable     string
	FKCol       string
	FKOnDelete  string
	FKOnUpdate  string
	Index       string // "", "normal", "desc", "unique", "iunique"
}

// ModelSchema holds the introspected schema of a Go model struct.
type ModelSchema struct {
	TableName string
	Columns   map[string]ModelColumn
	ColOrder  []string
	PK        string
	FKeys     []kormFkey
	MIndexes  map[string]string
	UIndexes  map[string]string
}

// ColumnDiff represents a specific column difference between code and DB.
type ColumnDiff struct {
	ColumnName string
	Action     string // "ADD", "DROP", "ALTER", "RENAME"
	OldDef     string
	NewDef     string
	Tags       []string
	IsFK       bool
	FKDetails  string
	Warning    string
	SQL        string
}

// TableDiff contains the full structural diff for a given table.
type TableDiff struct {
	TableName           string
	ExistsInDB          bool
	HasModel            bool
	AddedCols           []ColumnDiff
	DroppedCols         []ColumnDiff
	AlteredCols         []ColumnDiff
	RenamedCols         []ColumnDiff
	AddedIndexes        []string
	DroppedIndexes      []string
	TriggersNeedRefresh bool
	Warnings            []string
	SuggestedSQL        []string
}

// ExtractModelSchema extracts the table schema from a Go struct,
// applying the exact same filtering rules and tags as AutoMigrate & LinkModel.
func ExtractModelSchema(model any, dialect string, tableName string) (ModelSchema, error) {
	if model == nil {
		return ModelSchema{}, fmt.Errorf("model is nil for table %s", tableName)
	}

	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return ModelSchema{}, fmt.Errorf("model for %s is not a struct", tableName)
	}

	typeOfT := val.Type()
	schema := ModelSchema{
		TableName: tableName,
		Columns:   make(map[string]ModelColumn),
		ColOrder:  make([]string, 0, typeOfT.NumField()),
		FKeys:     make([]kormFkey, 0),
		MIndexes:  make(map[string]string),
		UIndexes:  make(map[string]string),
	}

	pk := ""

	for i := 0; i < typeOfT.NumField(); i++ {
		field := typeOfT.Field(i)
		colName := kstrct.ToSnakeCase(field.Name)

		// 1. Check tags for skip / ignored fields
		var tags []string
		if ftag, ok := field.Tag.Lookup("korm"); ok {
			rawTags := strings.Split(ftag, ";")
			for _, t := range rawTags {
				t = strings.TrimSpace(t)
				if t != "" {
					tags = append(tags, t)
				}
			}
		} else if ftag, ok := field.Tag.Lookup("kstrct"); ok {
			if strings.TrimSpace(ftag) == "-" {
				continue
			}
		}

		if len(tags) > 0 && (tags[0] == "-" || tags[0] == "skip") {
			continue
		}

		// 2. Determine field Go type
		fType := field.Type
		typeName := fType.String()
		if fType.Kind() == reflect.Ptr {
			typeName = fType.Elem().String()
		}

		// 3. Skip relation structs and slices of structs unless tagged with "json"
		hasJSON := false
		for _, t := range tags {
			if t == "json" {
				hasJSON = true
				break
			}
		}

		if !hasJSON {
			// Skip embedded/relation structs (e.g., School, models.School)
			if (unicode.IsUpper(rune(typeName[0])) || strings.Contains(typeName, ".")) &&
				!strings.HasSuffix(typeName, "Time") &&
				!(strings.HasPrefix(typeName, "map") || strings.HasPrefix(typeName, "*map")) {
				continue
			}

			// Skip slice of structs (e.g. []Student, []*Student)
			if strings.HasPrefix(typeName, "[]") || strings.HasPrefix(typeName, "*[]") {
				elemType := fType
				for elemType.Kind() == reflect.Ptr || elemType.Kind() == reflect.Slice {
					elemType = elemType.Elem()
				}
				if elemType.Kind() == reflect.Struct && !strings.HasSuffix(elemType.String(), "Time") {
					continue
				}
			}
		}

		// 4. Parse column properties and tags
		mCol := ModelColumn{
			Name:   colName,
			GoType: typeName,
			Tags:   tags,
		}

		for _, tag := range tags {
			switch {
			case tag == "pk" || tag == "autoinc" || colName == "id":
				mCol.IsPK = true
				if tag == "autoinc" || colName == "id" {
					mCol.IsAutoInc = true
				}
				pk = colName
			case tag == "notnull":
				mCol.IsNotNull = true
			case tag == "unique":
				mCol.IsUnique = true
				if mCol.Index == "" {
					mCol.Index = "unique"
				}
			case tag == "iunique":
				mCol.IsUnique = true
				mCol.IsIUnique = true
				mCol.Index = "iunique"
			case tag == "uindex":
				mCol.IsUnique = true
				if mCol.Index == "" {
					mCol.Index = "unique"
				}
			case strings.HasPrefix(tag, "uindex:"):
				mCol.IsUnique = true
				schema.UIndexes[colName] = strings.TrimPrefix(tag, "uindex:")
			case strings.HasPrefix(tag, "mindex:"):
				schema.MIndexes[colName] = strings.TrimPrefix(tag, "mindex:")
			case tag == "default":
				mCol.DefaultVal = defaultValForType(typeName)
			case strings.HasPrefix(tag, "default:"):
				mCol.DefaultVal = strings.TrimPrefix(tag, "default:")
			case tag == "now" || tag == "update":
				mCol.IsNotNull = true
				switch dialect {
				case POSTGRES, COCKROACH:
					mCol.DefaultVal = "extract(epoch from now())"
				case MYSQL, MARIA:
					mCol.DefaultVal = "0"
				default: // SQLite
					mCol.DefaultVal = "(strftime('%s', 'now'))"
				}
			case strings.HasPrefix(tag, "size:"):
				mCol.Size = strings.TrimPrefix(tag, "size:")
			case strings.HasPrefix(tag, "check:"):
				mCol.CheckExpr = strings.TrimPrefix(tag, "check:")
			case strings.HasPrefix(tag, "generated:"):
				mCol.GeneratedAs = strings.TrimPrefix(tag, "generated:")
			case tag == "index" || tag == "+index" || tag == "index+":
				if mCol.Index == "" {
					mCol.Index = "normal"
				}
			case tag == "-index" || tag == "index-":
				mCol.Index = "desc"
			case strings.HasPrefix(tag, "fk:"):
				// fk:schools.id:cascade:cascade
				mCol.IsFK = true
				parts := strings.Split(tag, ":")
				if len(parts) > 1 {
					targetParts := strings.Split(parts[1], ".")
					if len(targetParts) == 2 {
						mCol.FKTable = targetParts[0]
						mCol.FKCol = targetParts[1]
					}
				}
				if len(parts) > 2 {
					mCol.FKOnDelete = strings.ToUpper(parts[2])
				}
				if len(parts) > 3 {
					mCol.FKOnUpdate = strings.ToUpper(parts[3])
				}
				schema.FKeys = append(schema.FKeys, kormFkey{
					FromTableField: tableName + "." + colName,
					ToTableField:   parts[1],
					Unique:         mCol.IsUnique,
				})
			}
		}

		// 5. Generate dialect-specific SQL type
		mCol.SQLType = generateColumnSQLDefinition(mCol, dialect, false)
		schema.Columns[colName] = mCol
		schema.ColOrder = append(schema.ColOrder, colName)
	}

	// Fallback PK check if none explicitly tagged
	if pk == "" && len(schema.ColOrder) > 0 {
		firstCol := schema.ColOrder[0]
		if isFallbackPkName(firstCol) {
			pk = firstCol
			col := schema.Columns[firstCol]
			col.IsPK = true
			schema.Columns[firstCol] = col
		} else {
			// Synthetic id PK
			pk = "id"
			syntheticID := ModelColumn{
				Name:      "id",
				GoType:    "uint",
				IsPK:      true,
				IsAutoInc: true,
				SQLType:   generateAutoIncType(dialect),
				Tags:      []string{"pk", "autoinc"},
			}
			schema.Columns["id"] = syntheticID
			schema.ColOrder = append([]string{"id"}, schema.ColOrder...)
		}
	}
	schema.PK = pk

	return schema, nil
}

func isSQLiteDynamicDefault(defVal string) bool {
	trimmed := strings.TrimSpace(defVal)
	if trimmed == "" {
		return false
	}
	if strings.Contains(trimmed, "(") || strings.Contains(trimmed, ")") {
		return true
	}
	upper := strings.ToUpper(trimmed)
	switch upper {
	case "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "NOW", "NOW()":
		return true
	}
	return false
}

func defaultValForType(goType string) string {
	clean := strings.TrimPrefix(goType, "*")
	switch {
	case strings.Contains(clean, "int"), strings.Contains(clean, "float"):
		return "0"
	case clean == "bool":
		return "0"
	case clean == "string":
		return "''"
	case clean == "time.Time":
		return "0"
	default:
		return "''"
	}
}

func generateAutoIncType(dialect string) string {
	switch dialect {
	case POSTGRES, COCKROACH:
		return "SERIAL NOT NULL PRIMARY KEY"
	case MYSQL, MARIA:
		return "INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT"
	default: // SQLite
		return "INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"
	}
}

func generateColumnSQLDefinition(col ModelColumn, dialect string, isAlterAdd bool) string {
	if col.IsPK && col.IsAutoInc && !isAlterAdd {
		return generateAutoIncType(dialect)
	}

	baseType := ""
	cleanType := strings.TrimPrefix(col.GoType, "*")

	switch {
	case strings.Contains(cleanType, "int"):
		if strings.HasSuffix(cleanType, "64") {
			baseType = "BIGINT"
		} else {
			baseType = "INTEGER"
		}
	case cleanType == "string":
		hasText := false
		hasJSON := false
		for _, t := range col.Tags {
			switch t {
			case "text":
				hasText = true
			case "json":
				hasJSON = true
			}
		}
		if hasText {
			baseType = "TEXT"
		} else if hasJSON {
			switch dialect {
			case POSTGRES, COCKROACH:
				baseType = "JSONB"
			case MYSQL, MARIA:
				baseType = "JSON"
			default:
				baseType = "TEXT"
			}
		} else if col.Size != "" {
			baseType = fmt.Sprintf("VARCHAR(%s)", col.Size)
		} else {
			baseType = "VARCHAR(255)"
		}
	case cleanType == "bool":
		if dialect == SQLITE || dialect == "" || dialect == MYSQL || dialect == MARIA {
			baseType = "INTEGER"
		} else {
			baseType = "BOOLEAN"
		}
	case cleanType == "time.Time" || strings.HasSuffix(cleanType, "Time"):
		baseType = "BIGINT" // Korm stores time as epoch
	case strings.Contains(cleanType, "float"):
		if dialect == POSTGRES || dialect == COCKROACH {
			baseType = "DOUBLE PRECISION"
		} else {
			baseType = "DECIMAL(10,5)"
		}
	case strings.HasPrefix(cleanType, "[]") || strings.HasPrefix(cleanType, "map"):
		hasText := false
		hasJSON := false
		for _, t := range col.Tags {
			switch t {
			case "text":
				hasText = true
			case "json":
				hasJSON = true
			}
		}
		if hasJSON {
			switch dialect {
			case POSTGRES, COCKROACH:
				baseType = "JSONB"
			case MYSQL, MARIA:
				baseType = "JSON"
			default:
				baseType = "TEXT"
			}
		} else if hasText {
			baseType = "TEXT"
		} else {
			if dialect == POSTGRES || dialect == COCKROACH {
				baseType = "BYTEA"
			} else {
				baseType = "BLOB"
			}
		}
	default:
		hasText := false
		hasJSON := false
		for _, t := range col.Tags {
			switch t {
			case "text":
				hasText = true
			case "json":
				hasJSON = true
			}
		}
		if hasJSON {
			switch dialect {
			case POSTGRES, COCKROACH:
				baseType = "JSONB"
			case MYSQL, MARIA:
				baseType = "JSON"
			default:
				baseType = "TEXT"
			}
		} else if hasText {
			baseType = "TEXT"
		} else {
			if dialect == POSTGRES || dialect == COCKROACH {
				baseType = "BYTEA"
			} else {
				baseType = "BLOB"
			}
		}
	}

	var constraints []string
	if col.IsPK && !col.IsAutoInc && !isAlterAdd {
		constraints = append(constraints, "PRIMARY KEY")
	}
	if col.IsNotNull {
		constraints = append(constraints, "NOT NULL")
	}

	// In SQLite ALTER TABLE ADD COLUMN, UNIQUE is not allowed directly
	// (must be created via CREATE UNIQUE INDEX).
	if col.IsUnique && (!isAlterAdd || (dialect != SQLITE && dialect != "")) {
		if !col.IsIUnique {
			constraints = append(constraints, "UNIQUE")
		}
	}

	if col.DefaultVal != "" {
		if isAlterAdd && (dialect == SQLITE || dialect == "") && isSQLiteDynamicDefault(col.DefaultVal) {
			safeDef := defaultValForType(col.GoType)
			constraints = append(constraints, fmt.Sprintf("DEFAULT %s", safeDef))
		} else if cleanType == "bool" && (dialect == SQLITE || dialect == "" || dialect == MYSQL || dialect == MARIA) {
			switch col.DefaultVal {
			case "false":
				constraints = append(constraints, "DEFAULT 0")
			case "true":
				constraints = append(constraints, "DEFAULT 1")
			default:
				constraints = append(constraints, fmt.Sprintf("DEFAULT %s", col.DefaultVal))
			}
		} else if cleanType == "string" && !strings.HasPrefix(col.DefaultVal, "'") {
			constraints = append(constraints, fmt.Sprintf("DEFAULT '%s'", col.DefaultVal))
		} else {
			constraints = append(constraints, fmt.Sprintf("DEFAULT %s", col.DefaultVal))
		}
	} else if isAlterAdd && (dialect == SQLITE || dialect == "") && col.IsNotNull {
		safeDef := defaultValForType(col.GoType)
		constraints = append(constraints, fmt.Sprintf("DEFAULT %s", safeDef))
	}

	if col.CheckExpr != "" {
		constraints = append(constraints, fmt.Sprintf("CHECK (%s)", col.CheckExpr))
	}

	if col.GeneratedAs != "" {
		constraints = append(constraints, fmt.Sprintf("GENERATED ALWAYS AS (%s) STORED", col.GeneratedAs))
	}

	// If this is an ALTER TABLE ADD COLUMN and it has a Foreign Key:
	if isAlterAdd && col.IsFK && col.FKTable != "" && col.FKCol != "" {
		fkClause := fmt.Sprintf("REFERENCES `%s`(`%s`)", col.FKTable, col.FKCol)
		if col.FKOnDelete != "" {
			fkClause += " ON DELETE " + col.FKOnDelete
		}
		if col.FKOnUpdate != "" {
			fkClause += " ON UPDATE " + col.FKOnUpdate
		}
		constraints = append(constraints, fkClause)
	}

	if len(constraints) > 0 {
		return baseType + " " + strings.Join(constraints, " ")
	}
	return baseType
}

// CompareModelWithDB compares a single model with the physical database.
func CompareModelWithDB(schema ModelSchema, db *DatabaseEntity, reverseFK ...map[string][]string) TableDiff {
	var reverseFKMap map[string][]string
	if len(reverseFK) > 0 && reverseFK[0] != nil {
		reverseFKMap = reverseFK[0]
	} else {
		reverseFKMap = buildReverseFKMap(nil, db)
	}

	diff := TableDiff{
		TableName:    schema.TableName,
		HasModel:     true,
		AddedCols:    make([]ColumnDiff, 0),
		DroppedCols:  make([]ColumnDiff, 0),
		AlteredCols:  make([]ColumnDiff, 0),
		RenamedCols:  make([]ColumnDiff, 0),
		AddedIndexes: make([]string, 0),
		Warnings:     make([]string, 0),
		SuggestedSQL: make([]string, 0),
	}

	// 1. Check if table exists in DB
	allTables := GetAllTables(db.Name)
	tableFound := false
	for _, t := range allTables {
		if t == schema.TableName {
			tableFound = true
			break
		}
	}

	if !tableFound {
		diff.ExistsInDB = false
		diff.Warnings = append(diff.Warnings, fmt.Sprintf("Table '%s' does not exist in database.", schema.TableName))
		// Generate CREATE TABLE suggestion
		colDefs := make([]string, 0, len(schema.ColOrder))
		for _, colName := range schema.ColOrder {
			col := schema.Columns[colName]
			colDefs = append(colDefs, fmt.Sprintf("  `%s` %s", colName, col.SQLType))
		}
		for _, fk := range schema.FKeys {
			parts := strings.Split(fk.ToTableField, ".")
			if len(parts) == 2 {
				fromCol := strings.TrimPrefix(fk.FromTableField, schema.TableName+".")
				colDefs = append(colDefs, fmt.Sprintf("  FOREIGN KEY (`%s`) REFERENCES `%s`(`%s`)", fromCol, parts[0], parts[1]))
			}
		}
		createSQL := fmt.Sprintf("CREATE TABLE `%s` (\n%s\n);", schema.TableName, strings.Join(colDefs, ",\n"))
		diff.SuggestedSQL = append(diff.SuggestedSQL, createSQL)

		// Append unique / iunique indexes if needed
		for colName, col := range schema.Columns {
			if col.IsIUnique {
				idxSQL := generateIndexSQL(schema.TableName, colName, "iunique", db.Dialect)
				diff.SuggestedSQL = append(diff.SuggestedSQL, idxSQL)
			}
		}
		return diff
	}

	diff.ExistsInDB = true

	// 2. Fetch existing physical DB columns and foreign keys
	dbCols, _ := GetAllColumnsTypes(schema.TableName, db.Name)
	if dbCols == nil {
		dbCols = make(map[string]string)
	}
	physicalFKs := getPhysicalTableForeignKeys(db.Conn, schema.TableName, db.Dialect)

	// Fetch previous snapshot from _tables_infos if available
	var prevSnapshot *TableEntity
	prevEntities := GetTablesInfosFromDB(db.Name, schema.TableName)
	if len(prevEntities) > 0 {
		prevSnapshot = &prevEntities[0]
	}

	// 3. Detect ADDED columns (in Code Model, not in DB)
	for _, colName := range schema.ColOrder {
		col := schema.Columns[colName]
		if _, existsInDB := dbCols[colName]; !existsInDB {
			colSQLDef := generateColumnSQLDefinition(col, db.Dialect, true)
			colDiff := ColumnDiff{
				ColumnName: colName,
				Action:     "ADD",
				NewDef:     colSQLDef,
				Tags:       col.Tags,
				IsFK:       col.IsFK,
			}

			// Safety checks for ADD COLUMN
			if (db.Dialect == SQLITE || db.Dialect == "") && isSQLiteDynamicDefault(col.DefaultVal) {
				colDiff.Warning = fmt.Sprintf("Dynamic default '%s' replaced with static constant for SQLite ADD COLUMN compatibility.", col.DefaultVal)
				diff.Warnings = append(diff.Warnings, colDiff.Warning)
			} else if (db.Dialect == SQLITE || db.Dialect == "") && col.IsNotNull && col.DefaultVal == "" {
				colDiff.Warning = "Safe constant DEFAULT injected for SQLite ADD COLUMN with NOT NULL."
				diff.Warnings = append(diff.Warnings, colDiff.Warning)
			} else if col.IsNotNull && col.DefaultVal == "" {
				colDiff.Warning = fmt.Sprintf("Column '%s' is NOT NULL without a DEFAULT. Adding it to an existing table with data may fail.", colName)
				diff.Warnings = append(diff.Warnings, colDiff.Warning)
			}

			if col.IsFK {
				colDiff.FKDetails = fmt.Sprintf("FOREIGN KEY (`%s`) REFERENCES `%s`(`%s`)", colName, col.FKTable, col.FKCol)
				if col.FKOnDelete != "" {
					colDiff.FKDetails += " ON DELETE " + col.FKOnDelete
				}
				if col.FKOnUpdate != "" {
					colDiff.FKDetails += " ON UPDATE " + col.FKOnUpdate
				}
			}

			// Generate ALTER TABLE ADD COLUMN
			sqlAlter := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s;", schema.TableName, colName, colSQLDef)
			colDiff.SQL = sqlAlter
			diff.AddedCols = append(diff.AddedCols, colDiff)
			diff.SuggestedSQL = append(diff.SuggestedSQL, sqlAlter)

			// If column requires a unique or case-insensitive unique index (especially for SQLite):
			if col.IsIUnique || (col.IsUnique && (db.Dialect == SQLITE || db.Dialect == "")) {
				idxType := "unique"
				if col.IsIUnique {
					idxType = "iunique"
				}
				idxSQL := generateIndexSQL(schema.TableName, colName, idxType, db.Dialect)
				diff.AddedIndexes = append(diff.AddedIndexes, idxSQL)
				diff.SuggestedSQL = append(diff.SuggestedSQL, idxSQL)
			} else if col.Index == "normal" || col.Index == "desc" {
				idxSQL := generateIndexSQL(schema.TableName, colName, col.Index, db.Dialect)
				diff.AddedIndexes = append(diff.AddedIndexes, idxSQL)
				diff.SuggestedSQL = append(diff.SuggestedSQL, idxSQL)
			}
		}
	}

	// 4. Detect DROPPED columns (in DB, not in Code Model)
	for dbColName, dbType := range dbCols {
		if strings.HasPrefix(dbColName, "_") {
			// ignore internal columns
			continue
		}
		if _, existsInModel := schema.Columns[dbColName]; !existsInModel {
			targetField := schema.TableName + "." + dbColName
			refDeps := reverseFKMap[targetField]

			warningMsg := ""
			if len(refDeps) > 0 {
				warningMsg = fmt.Sprintf("FK dependency in: %s", strings.Join(refDeps, ", "))
				diff.Warnings = append(diff.Warnings, fmt.Sprintf("Column '%s.%s' has FK dependency in: %s", schema.TableName, dbColName, strings.Join(refDeps, ", ")))
			}

			// Check if this dropped column was itself a foreign key
			droppedFKTarget := physicalFKs[dbColName]
			if droppedFKTarget == "" && prevSnapshot != nil {
				for _, fk := range prevSnapshot.Fkeys {
					if fk.FromTableField == schema.TableName+"."+dbColName {
						droppedFKTarget = fk.ToTableField
						break
					}
				}
			}

			colDiff := ColumnDiff{
				ColumnName: dbColName,
				Action:     "DROP",
				OldDef:     dbType,
				Warning:    warningMsg,
				IsFK:       droppedFKTarget != "",
				FKDetails:  droppedFKTarget,
			}

			// In MySQL and Postgres, drop foreign key constraint first if one existed
			if droppedFKTarget != "" {
				fkConstraintName := fmt.Sprintf("fk_%s_%s", schema.TableName, dbColName)
				switch db.Dialect {
				case MYSQL, MARIA:
					diff.SuggestedSQL = append(diff.SuggestedSQL, fmt.Sprintf("ALTER TABLE `%s` DROP FOREIGN KEY `%s`;", schema.TableName, fkConstraintName))
				case POSTGRES, COCKROACH:
					diff.SuggestedSQL = append(diff.SuggestedSQL, fmt.Sprintf("ALTER TABLE \"%s\" DROP CONSTRAINT IF EXISTS \"%s\";", schema.TableName, fkConstraintName))
				}
			}

			switch db.Dialect {
			case POSTGRES, COCKROACH:
				colDiff.SQL = fmt.Sprintf("ALTER TABLE \"%s\" DROP COLUMN \"%s\";", schema.TableName, dbColName)
			default:
				colDiff.SQL = fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`;", schema.TableName, dbColName)
			}
			diff.DroppedCols = append(diff.DroppedCols, colDiff)
			diff.SuggestedSQL = append(diff.SuggestedSQL, colDiff.SQL)
		}
	}

	// 5. Detect possible RENAMED columns (Heuristic: 1 dropped + 1 added with matching previous snapshot)
	if len(diff.DroppedCols) == 1 && len(diff.AddedCols) == 1 && prevSnapshot != nil {
		droppedCol := diff.DroppedCols[0]
		addedCol := diff.AddedCols[0]
		renameSQL := fmt.Sprintf("ALTER TABLE `%s` RENAME COLUMN `%s` TO `%s`;", schema.TableName, droppedCol.ColumnName, addedCol.ColumnName)

		renameDiff := ColumnDiff{
			ColumnName: addedCol.ColumnName,
			Action:     "RENAME",
			OldDef:     droppedCol.ColumnName,
			NewDef:     addedCol.ColumnName,
			Tags:       schema.Columns[addedCol.ColumnName].Tags,
			SQL:        renameSQL,
		}

		targetField := schema.TableName + "." + droppedCol.ColumnName
		if refDeps, ok := reverseFKMap[targetField]; ok && len(refDeps) > 0 {
			renameDiff.Warning = fmt.Sprintf("FK dependency in: %s", strings.Join(refDeps, ", "))
			diff.Warnings = append(diff.Warnings, renameDiff.Warning)
		}
		diff.RenamedCols = append(diff.RenamedCols, renameDiff)

		// When a rename is detected, replace the noisy ADD+DROP suggestions with clean RENAME + Index
		cleanSQL := []string{renameSQL}
		for _, idxSQL := range diff.AddedIndexes {
			cleanSQL = append(cleanSQL, idxSQL)
		}
		diff.SuggestedSQL = cleanSQL
	}

	// 6. Detect ALTERED columns (type / size / nullability changes on existing columns)
	for colName, col := range schema.Columns {
		if dbType, exists := dbCols[colName]; exists {
			if !isTypeCompatible(col, dbType, db.Dialect) {
				colDiff := ColumnDiff{
					ColumnName: colName,
					Action:     "ALTER",
					OldDef:     dbType,
					NewDef:     col.SQLType,
					Tags:       col.Tags,
					Warning:    fmt.Sprintf("Type mismatch for '%s': DB has '%s', Go model expects '%s'.", colName, dbType, col.SQLType),
				}
				diff.Warnings = append(diff.Warnings, colDiff.Warning)

				switch db.Dialect {
				case POSTGRES, COCKROACH:
					colDiff.SQL = fmt.Sprintf("ALTER TABLE `%s` ALTER COLUMN `%s` TYPE %s;", schema.TableName, colName, col.SQLType)
					diff.SuggestedSQL = append(diff.SuggestedSQL, colDiff.SQL)
				case MYSQL, MARIA:
					colDiff.SQL = fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` %s;", schema.TableName, colName, col.SQLType)
					diff.SuggestedSQL = append(diff.SuggestedSQL, colDiff.SQL)
				default: // SQLite does not support ALTER COLUMN
					diff.Warnings = append(diff.Warnings, fmt.Sprintf("SQLite cannot alter column type for '%s'. Recreating the table via a temp table is required.", colName))
				}
				diff.AlteredCols = append(diff.AlteredCols, colDiff)
			}
		}
	}

	// 7. Detect Index Diffs on existing columns
	existingIndexes := getExistingIndexes(db.Conn, schema.TableName, db.Dialect)
	indexedColumns := getIndexedColumns(db.Conn, schema.TableName, db.Dialect)
	for colName, col := range schema.Columns {
		// Only check if column already exists in DB (not freshly added)
		if _, existsInDB := dbCols[colName]; existsInDB {
			if col.Index != "" {
				expectedIdxName := fmt.Sprintf("idx_%s_%s", schema.TableName, colName)
				if !containsString(existingIndexes, expectedIdxName) && !indexedColumns[colName] {
					idxSQL := generateIndexSQL(schema.TableName, colName, col.Index, db.Dialect)
					if idxSQL != "" {
						diff.AddedIndexes = append(diff.AddedIndexes, idxSQL)
						diff.SuggestedSQL = append(diff.SuggestedSQL, idxSQL)
					}
				}
			}
		}
	}

	// 8. Refresh triggers only if columns were dropped or renamed on an existing table.
	// In SQLite and MySQL, triggers hardcode column names and will CRASH on write operations
	// if referencing deleted/renamed columns. ADD COLUMN does not break existing triggers.
	hasBreakingChanges := len(diff.DroppedCols) > 0 || len(diff.RenamedCols) > 0
	if diff.ExistsInDB && hasBreakingChanges {
		trigSQL := buildTriggerRefreshSQL(schema, db.Dialect, dbCols)
		if len(trigSQL) > 0 {
			diff.TriggersNeedRefresh = true
			diff.SuggestedSQL = append(diff.SuggestedSQL, trigSQL...)
		}
	}

	return diff
}

func generateIndexSQL(tableName, colName, indexType, dialect string) string {
	idxName := fmt.Sprintf("idx_%s_%s", tableName, colName)
	switch indexType {
	case "iunique":
		if dialect == MYSQL || dialect == MARIA {
			return fmt.Sprintf("CREATE UNIQUE INDEX %s ON `%s` (`%s`);", idxName, tableName, colName)
		}
		return fmt.Sprintf("CREATE UNIQUE INDEX %s ON `%s` (LOWER(`%s`));", idxName, tableName, colName)
	case "unique":
		return fmt.Sprintf("CREATE UNIQUE INDEX %s ON `%s` (`%s`);", idxName, tableName, colName)
	case "desc":
		return fmt.Sprintf("CREATE INDEX %s ON `%s` (`%s` DESC);", idxName, tableName, colName)
	case "normal":
		return fmt.Sprintf("CREATE INDEX %s ON `%s` (`%s`);", idxName, tableName, colName)
	default:
		return ""
	}
}

func buildTriggerRefreshSQL(schema ModelSchema, dialect string, dbCols map[string]string) []string {
	var stmts []string
	tableName := schema.TableName

	// 1. Change triggers (for SQLite & MySQL where columns are hardcoded into trigger body)
	if tableName != "users" && !strings.HasPrefix(tableName, "_") {
		switch dialect {
		case SQLITE, "":
			stmts = append(stmts,
				fmt.Sprintf("DROP TRIGGER IF EXISTS `%s_trig_insert`;", tableName),
				fmt.Sprintf("DROP TRIGGER IF EXISTS `%s_trig_update`;", tableName),
				fmt.Sprintf("DROP TRIGGER IF EXISTS `%s_trig_delete`;", tableName),
			)
			var newPairs, oldPairs []string
			colsToUse := make([]string, 0, len(schema.Columns))
			if len(schema.ColOrder) > 0 {
				for _, c := range schema.ColOrder {
					if _, ok := schema.Columns[c]; ok {
						colsToUse = append(colsToUse, c)
					}
				}
			} else {
				for c := range schema.Columns {
					colsToUse = append(colsToUse, c)
				}
				sort.Strings(colsToUse)
			}
			for _, col := range colsToUse {
				newPairs = append(newPairs, fmt.Sprintf("'%s',NEW.`%s`", col, col))
				oldPairs = append(oldPairs, fmt.Sprintf("'%s',OLD.`%s`", col, col))
			}
			newFields := strings.Join(newPairs, ",")
			oldFields := strings.Join(oldPairs, ",")

			stmts = append(stmts,
				fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS `%s_trig_insert` AFTER INSERT ON `%s` FOR EACH ROW BEGIN INSERT INTO `_triggers_queue`(data) VALUES (json_object('operation','insert','table','%s','data',json_object(%s))); END;", tableName, tableName, tableName, newFields),
				fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS `%s_trig_update` AFTER UPDATE ON `%s` FOR EACH ROW BEGIN INSERT INTO `_triggers_queue`(data) VALUES (json_object('operation','update','table','%s','old',json_object(%s),'new',json_object(%s))); END;", tableName, tableName, tableName, oldFields, newFields),
				fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS `%s_trig_delete` AFTER DELETE ON `%s` FOR EACH ROW BEGIN INSERT INTO `_triggers_queue`(data) VALUES (json_object('operation','delete','table','%s','data',json_object(%s))); END;", tableName, tableName, tableName, oldFields),
			)
		case MYSQL, MARIA:
			stmts = append(stmts,
				fmt.Sprintf("DROP TRIGGER IF EXISTS `%s_trig_insert`;", tableName),
				fmt.Sprintf("DROP TRIGGER IF EXISTS `%s_trig_update`;", tableName),
				fmt.Sprintf("DROP TRIGGER IF EXISTS `%s_trig_delete`;", tableName),
			)
			var newPairs, oldPairs []string
			colsToUse := make([]string, 0, len(schema.Columns))
			for c := range schema.Columns {
				colsToUse = append(colsToUse, c)
			}
			sort.Strings(colsToUse)
			for _, col := range colsToUse {
				newPairs = append(newPairs, fmt.Sprintf("'%s',NEW.`%s`", col, col))
				oldPairs = append(oldPairs, fmt.Sprintf("'%s',OLD.`%s`", col, col))
			}
			newFields := strings.Join(newPairs, ",")
			oldFields := strings.Join(oldPairs, ",")

			stmts = append(stmts,
				fmt.Sprintf("CREATE TRIGGER `%s_trig_insert` AFTER INSERT ON `%s` FOR EACH ROW BEGIN INSERT INTO `_triggers_queue`(data) VALUES (JSON_OBJECT('operation','insert','table','%s','data',JSON_OBJECT(%s))); END;", tableName, tableName, tableName, newFields),
				fmt.Sprintf("CREATE TRIGGER `%s_trig_update` AFTER UPDATE ON `%s` FOR EACH ROW BEGIN INSERT INTO `_triggers_queue`(data) VALUES (JSON_OBJECT('operation','update','table','%s','old',JSON_OBJECT(%s),'new',JSON_OBJECT(%s))); END;", tableName, tableName, tableName, oldFields, newFields),
				fmt.Sprintf("CREATE TRIGGER `%s_trig_delete` AFTER DELETE ON `%s` FOR EACH ROW BEGIN INSERT INTO `_triggers_queue`(data) VALUES (JSON_OBJECT('operation','delete','table','%s','data',JSON_OBJECT(%s))); END;", tableName, tableName, tableName, oldFields),
			)
		}
	}

	// 2. updated_at trigger
	pk := schema.PK
	if pk == "" {
		pk = "id"
	}
	_, hasUpdatedAtInModel := schema.Columns["updated_at"]
	_, hadUpdatedAtInDB := dbCols["updated_at"]

	if hadUpdatedAtInDB && !hasUpdatedAtInModel {
		// Column was dropped
		switch dialect {
		case POSTGRES, COCKROACH:
			stmts = append(stmts,
				fmt.Sprintf(`DROP TRIGGER IF EXISTS "%s_updated_at_update_trig" ON "%s";`, tableName, tableName),
				fmt.Sprintf(`DROP FUNCTION IF EXISTS "%s_updated_at_trig"();`, tableName),
			)
		default:
			stmts = append(stmts, fmt.Sprintf("DROP TRIGGER IF EXISTS `%s_update_trig`;", tableName))
		}
	} else if hasUpdatedAtInModel {
		switch dialect {
		case SQLITE, "":
			stmts = append(stmts,
				fmt.Sprintf("DROP TRIGGER IF EXISTS `%s_update_trig`;", tableName),
				fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS `%s_update_trig` AFTER UPDATE ON `%s` BEGIN UPDATE `%s` SET `updated_at` = strftime('%%s', 'now') WHERE `%s` = NEW.`%s`; END;", tableName, tableName, tableName, pk, pk),
			)
		case POSTGRES, COCKROACH:
			stmts = append(stmts,
				fmt.Sprintf(`CREATE OR REPLACE FUNCTION "%s_updated_at_trig"() RETURNS trigger AS $$ BEGIN NEW."updated_at" = extract(epoch from now());RETURN NEW; END;$$ LANGUAGE plpgsql;`, tableName),
				fmt.Sprintf(`CREATE OR REPLACE TRIGGER "%s_updated_at_update_trig" BEFORE UPDATE ON public."%s" FOR EACH ROW EXECUTE PROCEDURE "%s_updated_at_trig"();`, tableName, tableName, tableName),
			)
		}
	}

	return stmts
}

func getExistingIndexes(conn *sql.DB, tableName, dialect string) []string {
	if conn == nil {
		return nil
	}
	indexes := []string{}
	switch dialect {
	case POSTGRES, COCKROACH:
		rows, err := conn.Query("SELECT indexname FROM pg_indexes WHERE tablename = $1", tableName)
		if err != nil {
			return nil
		}
		defer rows.Close()
		var idx string
		for rows.Next() {
			if err := rows.Scan(&idx); err == nil {
				indexes = append(indexes, idx)
			}
		}
		lg.CheckError(rows.Err())
	case MYSQL, MARIA:
		rows, err := conn.Query("SELECT DISTINCT index_name FROM information_schema.statistics WHERE table_name = ?", tableName)
		if err != nil {
			return nil
		}
		defer rows.Close()
		var idx string
		for rows.Next() {
			if err := rows.Scan(&idx); err == nil {
				indexes = append(indexes, idx)
			}
		}
		lg.CheckError(rows.Err())
	default: // SQLite
		rows, err := conn.Query("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name = ?", tableName)
		if err != nil {
			return nil
		}
		defer rows.Close()
		var idx string
		for rows.Next() {
			if err := rows.Scan(&idx); err == nil {
				indexes = append(indexes, idx)
			}
		}
		lg.CheckError(rows.Err())
	}
	return indexes
}

func containsString(slice []string, val string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, val) {
			return true
		}
	}
	return false
}

func getIndexedColumns(conn *sql.DB, tableName, dialect string) map[string]bool {
	indexedCols := make(map[string]bool)
	if conn == nil {
		return indexedCols
	}
	switch dialect {
	case SQLITE, "":
		rows, err := conn.Query(fmt.Sprintf("PRAGMA index_list(`%s`);", tableName))
		if err != nil {
			return indexedCols
		}
		defer rows.Close()
		var seq, unique, partial int
		var idxName, origin string
		var idxNames []string
		for rows.Next() {
			if err := rows.Scan(&seq, &idxName, &unique, &origin, &partial); err == nil {
				idxNames = append(idxNames, idxName)
			}
		}
		lg.CheckError(rows.Err())

		for _, name := range idxNames {
			infoRows, err := conn.Query(fmt.Sprintf("PRAGMA index_info(`%s`);", name))
			if err != nil {
				continue
			}
			var seqno, cid int
			var colName string
			for infoRows.Next() {
				if err := infoRows.Scan(&seqno, &cid, &colName); err == nil {
					indexedCols[colName] = true
				}
			}
			lg.CheckError(infoRows.Err())
			infoRows.Close()
		}
	case MYSQL, MARIA:
		rows, err := conn.Query("SELECT DISTINCT column_name FROM information_schema.statistics WHERE table_name = ?", tableName)
		if err == nil {
			defer rows.Close()
			var cName string
			for rows.Next() {
				if err := rows.Scan(&cName); err == nil {
					indexedCols[cName] = true
				}
			}
			lg.CheckError(rows.Err())
		}
	case POSTGRES, COCKROACH:
		query := `SELECT a.attname
			FROM pg_class t
			JOIN pg_index ix ON t.oid = ix.indrelid
			JOIN pg_class i ON i.oid = ix.indexrelid
			JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
			WHERE t.relname = $1;`
		rows, err := conn.Query(query, tableName)
		if err == nil {
			defer rows.Close()
			var cName string
			for rows.Next() {
				if err := rows.Scan(&cName); err == nil {
					indexedCols[cName] = true
				}
			}
			lg.CheckError(rows.Err())
		}
	}
	return indexedCols
}

func normalizeSQLType(raw string) string {
	t := strings.ToUpper(strings.TrimSpace(raw))
	if t == "" {
		return ""
	}
	switch {
	case strings.HasPrefix(t, "DOUBLE PRECISION"):
		return "DOUBLE PRECISION"
	case strings.HasPrefix(t, "CHARACTER VARYING"):
		return "VARCHAR"
	case strings.HasPrefix(t, "TIMESTAMP WITHOUT TIME ZONE"), strings.HasPrefix(t, "TIMESTAMP WITH TIME ZONE"):
		return "TIMESTAMP"
	}
	fields := strings.Fields(t)
	if len(fields) > 0 {
		t = fields[0]
	}
	if idx := strings.Index(t, "("); idx != -1 {
		t = t[:idx]
	}
	return strings.TrimSpace(t)
}

func isTypeCompatible(col ModelColumn, dbType string, dialect string) bool {
	actual := normalizeSQLType(dbType)
	expected := normalizeSQLType(col.SQLType)

	if actual == "" || expected == "" {
		return true
	}
	if actual == expected {
		return true
	}

	cleanGoType := strings.TrimPrefix(col.GoType, "*")

	// 1. Booleans: in SQLite/MySQL/Postgres, boolean can be represented as INTEGER, TINYINT, BOOLEAN, etc.
	if cleanGoType == "bool" || expected == "BOOLEAN" || expected == "BOOL" {
		switch actual {
		case "BOOLEAN", "BOOL", "TINYINT", "INTEGER", "INT", "SMALLINT", "BIT", "NUMERIC":
			return true
		}
	}

	// 2. Floats & Decimals: REAL, FLOAT, DOUBLE, DOUBLE PRECISION, DECIMAL, NUMERIC
	if strings.Contains(cleanGoType, "float") || isFloatSQLType(expected) {
		if isFloatSQLType(actual) {
			return true
		}
	}

	// 3. Integers
	if strings.Contains(cleanGoType, "int") || isIntegerSQLType(expected) {
		if dialect == SQLITE || dialect == "" {
			if isIntegerSQLType(actual) || actual == "NUMERIC" {
				return true
			}
		} else {
			if isIntegerSQLType(actual) {
				if (actual == "INT" && expected == "INTEGER") || (actual == "INTEGER" && expected == "INT") {
					return true
				}
				if actual == "SERIAL" || expected == "SERIAL" {
					return true
				}
			}
		}
	}

	// 4. Strings / Text / JSON
	if cleanGoType == "string" || isStringSQLType(expected) {
		if isStringSQLType(actual) {
			return true
		}
	}

	// 5. Slices / Arrays / Maps / Byte arrays (can be stored as BLOB, BYTEA, TEXT, JSON, JSONB, VARBINARY)
	if strings.HasPrefix(cleanGoType, "[]") || strings.HasPrefix(cleanGoType, "map") {
		switch actual {
		case "BLOB", "BYTEA", "TEXT", "JSON", "JSONB", "VARCHAR", "VARBINARY":
			return true
		}
	}

	// 6. Time (KORM stores unix epoch timestamp or native timestamp)
	if cleanGoType == "time.Time" || strings.HasSuffix(cleanGoType, "Time") {
		switch actual {
		case "BIGINT", "INTEGER", "INT", "NUMERIC", "TIMESTAMP", "DATETIME":
			return true
		}
	}

	return false
}

func isFloatSQLType(t string) bool {
	switch t {
	case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "DECIMAL", "NUMERIC":
		return true
	}
	return false
}

func isIntegerSQLType(t string) bool {
	switch t {
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT", "SERIAL", "BIGSERIAL":
		return true
	}
	return false
}

func isStringSQLType(t string) bool {
	switch t {
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER", "CLOB", "STRING", "JSON", "JSONB":
		return true
	}
	return false
}

func getPhysicalTableForeignKeys(conn *sql.DB, tableName, dialect string) map[string]string {
	res := make(map[string]string)
	if conn == nil {
		return res
	}

	switch dialect {
	case POSTGRES, COCKROACH:
		query := `SELECT kcu.column_name, ccu.table_name, ccu.column_name 
			FROM information_schema.table_constraints AS tc 
			JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema 
			JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name 
			WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;`
		rows, err := conn.Query(query, tableName)
		if err != nil {
			return res
		}
		defer rows.Close()
		var col, toTable, toCol string
		for rows.Next() {
			if err := rows.Scan(&col, &toTable, &toCol); err == nil {
				res[col] = toTable + "." + toCol
			}
		}
		lg.CheckError(rows.Err())
	case MYSQL, MARIA:
		query := `SELECT column_name, referenced_table_name, referenced_column_name 
			FROM information_schema.key_column_usage 
			WHERE table_name = ? AND referenced_table_name IS NOT NULL;`
		rows, err := conn.Query(query, tableName)
		if err != nil {
			return res
		}
		defer rows.Close()
		var col, toTable, toCol string
		for rows.Next() {
			if err := rows.Scan(&col, &toTable, &toCol); err == nil {
				res[col] = toTable + "." + toCol
			}
		}
		lg.CheckError(rows.Err())
	default: // SQLite
		rows, err := conn.Query(fmt.Sprintf("PRAGMA foreign_key_list(`%s`);", tableName))
		if err != nil {
			return res
		}
		defer rows.Close()
		var id, seq int
		var toTable, fromCol, toCol, onUpdate, onDelete, match string
		for rows.Next() {
			if err := rows.Scan(&id, &seq, &toTable, &fromCol, &toCol, &onUpdate, &onDelete, &match); err == nil {
				res[fromCol] = toTable + "." + toCol
			}
		}
		lg.CheckError(rows.Err())
	}
	return res
}

// buildReverseFKMap builds a reverse lookup map: targetField (e.g. "schools.new") -> []referencingDescription
func buildReverseFKMap(models map[string]any, db *DatabaseEntity) map[string][]string {
	reverseMap := make(map[string][]string)

	addRef := func(target, ref string) {
		for _, existing := range reverseMap[target] {
			if existing == ref {
				return
			}
		}
		reverseMap[target] = append(reverseMap[target], ref)
	}

	// 1. From registered Go models
	for tName, model := range models {
		if strings.HasPrefix(tName, "_") {
			continue
		}
		schema, err := ExtractModelSchema(model, db.Dialect, tName)
		if err == nil {
			for _, fk := range schema.FKeys {
				fromCol := strings.TrimPrefix(fk.FromTableField, tName+".")
				refDesc := fmt.Sprintf("table '%s' (column '%s')", tName, fromCol)
				addRef(fk.ToTableField, refDesc)
			}
		}
	}

	// 2. From DB _tables_infos snapshot
	dbEntities := GetTablesInfosFromDB(db.Name)
	for _, te := range dbEntities {
		for _, fk := range te.Fkeys {
			fromCol := strings.TrimPrefix(fk.FromTableField, te.Name+".")
			refDesc := fmt.Sprintf("table '%s' (column '%s')", te.Name, fromCol)
			addRef(fk.ToTableField, refDesc)
		}
	}

	// 3. From in-memory db.Tables
	for _, te := range db.Tables {
		for _, fk := range te.Fkeys {
			fromCol := strings.TrimPrefix(fk.FromTableField, te.Name+".")
			refDesc := fmt.Sprintf("table '%s' (column '%s')", te.Name, fromCol)
			addRef(fk.ToTableField, refDesc)
		}
	}

	return reverseMap
}

// CheckSchema performs a full schema inspection across all registered models.
func CheckSchema(dbName ...string) ([]TableDiff, error) {
	dName := ""
	if len(dbName) > 0 {
		dName = dbName[0]
	} else if len(databases) > 0 {
		dName = databases[0].Name
	} else {
		return nil, fmt.Errorf("no database connected")
	}

	db, err := GetMemoryDatabase(dName)
	if err != nil || db == nil {
		return nil, fmt.Errorf("database %s not found in memory", dName)
	}

	mutexModelTablename.RLock()
	modelsCopy := make(map[string]any, len(mModelTablename))
	for k, v := range mModelTablename {
		modelsCopy[k] = v
	}
	mutexModelTablename.RUnlock()

	// Pre-build reverse foreign key dependency map across all models & DB
	reverseFKMap := buildReverseFKMap(modelsCopy, db)

	var diffs []TableDiff

	// 1. Inspect registered models
	for tableName, model := range modelsCopy {
		if strings.HasPrefix(tableName, "_") {
			continue
		}
		schema, err := ExtractModelSchema(model, db.Dialect, tableName)
		if err != nil {
			lg.ErrorC("unable to extract model schema", "table", tableName, "err", err)
			continue
		}
		tDiff := CompareModelWithDB(schema, db, reverseFKMap)
		diffs = append(diffs, tDiff)
	}

	// Sort diffs by table name for deterministic display
	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].TableName < diffs[j].TableName
	})

	return diffs, nil
}

// PrintSchemaDiff prints the schema diff to stdout in a clean, colored format.
func PrintSchemaDiff(diffs []TableDiff) {
	fmt.Println()
	fmt.Printf("\033[1;36m=== [KORM SCHEMA CHECK (DRY-RUN)] ===\033[0m\n")
	fmt.Printf("\033[0;33mChecking diff between Go models and physical database...\033[0m\n\n")

	totalChanges := 0

	for _, d := range diffs {
		hasChanges := !d.ExistsInDB || len(d.AddedCols) > 0 || len(d.DroppedCols) > 0 ||
			len(d.AlteredCols) > 0 || len(d.AddedIndexes) > 0 || len(d.RenamedCols) > 0

		if !hasChanges {
			fmt.Printf("  \033[1;32m✔ Table '%s' is up to date.\033[0m\n", d.TableName)
			continue
		}

		totalChanges++
		fmt.Printf("  \033[1;35m● Table '%s'\033[0m:\n", d.TableName)

		if !d.ExistsInDB {
			fmt.Printf("    \033[1;33m[NEW TABLE]\033[0m Table does not exist in DB.\n")
		}

		if len(d.RenamedCols) > 0 {
			for _, ren := range d.RenamedCols {
				tagInfo := ""
				if len(ren.Tags) > 0 {
					tagInfo = fmt.Sprintf(" \033[0;36m[tags: %s]\033[0m", strings.Join(ren.Tags, ", "))
				}
				fmt.Printf("    \033[1;34m[RENAME]\033[0m `%s` ➔ `%s`%s\n", ren.OldDef, ren.NewDef, tagInfo)
				if ren.Warning != "" {
					fmt.Printf("      \033[0;33m↳ ⚠ %s\033[0m\n", ren.Warning)
				}
			}
		} else {
			for _, add := range d.AddedCols {
				tagInfo := ""
				if len(add.Tags) > 0 {
					tagInfo = fmt.Sprintf(" \033[0;36m[tags: %s]\033[0m", strings.Join(add.Tags, ", "))
				}
				fmt.Printf("    \033[1;32m[+ ADD COLUMN]\033[0m `%s` (%s)%s\n", add.ColumnName, add.NewDef, tagInfo)
				if add.IsFK {
					fmt.Printf("      \033[1;34m↳ %s\033[0m\n", add.FKDetails)
				}
				if add.Warning != "" {
					fmt.Printf("      \033[0;33m↳ ⚠ %s\033[0m\n", add.Warning)
				}
			}

			for _, drop := range d.DroppedCols {
				fkTag := ""
				if drop.IsFK {
					fkTag = fmt.Sprintf(" \033[0;34m[FK: %s]\033[0m", drop.FKDetails)
				}
				fmt.Printf("    \033[1;31m[- DROP COLUMN]\033[0m `%s` (was %s)%s\n", drop.ColumnName, drop.OldDef, fkTag)
				if drop.Warning != "" {
					fmt.Printf("      \033[0;33m↳ ⚠ %s\033[0m\n", drop.Warning)
				}
			}
		}

		for _, alt := range d.AlteredCols {
			tagInfo := ""
			if len(alt.Tags) > 0 {
				tagInfo = fmt.Sprintf(" \033[0;36m[tags: %s]\033[0m", strings.Join(alt.Tags, ", "))
			}
			fmt.Printf("    \033[1;33m[~ ALTER COLUMN]\033[0m `%s` (DB: %s -> Model: %s)%s\n", alt.ColumnName, alt.OldDef, alt.NewDef, tagInfo)
			if alt.Warning != "" {
				fmt.Printf("      \033[0;33m↳ ⚠ %s\033[0m\n", alt.Warning)
			}
		}

		for _, idx := range d.AddedIndexes {
			fmt.Printf("    \033[1;32m[+ ADD INDEX]\033[0m %s\n", idx)
		}

		if d.TriggersNeedRefresh {
			fmt.Printf("    \033[1;34m[↻ REFRESH TRIGGERS]\033[0m Change triggers will be refreshed for updated columns\n")
		}

		if len(d.SuggestedSQL) > 0 {
			fmt.Printf("\n    \033[1;36mProposed SQL queries (not executed):\033[0m\n")
			for _, sqlStr := range d.SuggestedSQL {
				for _, line := range strings.Split(sqlStr, "\n") {
					fmt.Printf("      \033[0;32m%s\033[0m\n", line)
				}
			}
		}
		fmt.Println()
	}

	if totalChanges == 0 {
		fmt.Printf("\n\033[1;32mEverything is in sync! No schema differences found.\033[0m\n\n")
	} else {
		fmt.Printf("\n\033[1;33mTotal tables with changes: %d. (Nothing was executed).\033[0m\n\n", totalChanges)
	}
}

// ApplySchemaDiff executes the suggested SQL statements in diffs against the specified database.
func ApplySchemaDiff(diffs []TableDiff, dbName string) (int, error) {
	db, err := GetMemoryDatabase(dbName)
	if err != nil || db == nil {
		return 0, fmt.Errorf("database '%s' not found", dbName)
	}
	applied := 0
	for _, d := range diffs {
		for _, sqlStr := range d.SuggestedSQL {
			_, err := db.Conn.Exec(sqlStr)
			if err != nil {
				return applied, fmt.Errorf("table '%s': error executing '%s': %w", d.TableName, sqlStr, err)
			}
			applied++
		}
	}
	flushCache()
	return applied, nil
}

// handleDiffCommand is invoked by the shell "diff" command.
func handleDiffCommand(commands []string) {
	dbName := ""
	if usedDB.Name != "" {
		dbName = usedDB.Name
	}

	apply := false
	filteredCommands := []string{}
	for _, c := range commands {
		if c == "-a" || c == "--apply" || c == "-y" {
			apply = true
		} else {
			filteredCommands = append(filteredCommands, c)
		}
	}

	diffs, err := CheckSchema(dbName)
	if err != nil {
		fmt.Printf(red, fmt.Sprintf("Error checking schema: %v\n", err))
		return
	}

	// Filter by specific table if provided: "diff students"
	if len(filteredCommands) > 1 && filteredCommands[1] != "" {
		targetTable := filteredCommands[1]
		filtered := []TableDiff{}
		for _, d := range diffs {
			if d.TableName == targetTable {
				filtered = append(filtered, d)
			}
		}
		if len(filtered) == 0 {
			fmt.Printf(yellow, fmt.Sprintf("Table '%s' not found in registered models.\n", targetTable))
			return
		}
		diffs = filtered
	}

	PrintSchemaDiff(diffs)

	hasChanges := false
	totalSQL := 0
	for _, d := range diffs {
		totalSQL += len(d.SuggestedSQL)
		if !d.ExistsInDB || len(d.AddedCols) > 0 || len(d.DroppedCols) > 0 ||
			len(d.AlteredCols) > 0 || len(d.AddedIndexes) > 0 || len(d.RenamedCols) > 0 {
			hasChanges = true
		}
	}

	if !hasChanges || totalSQL == 0 {
		return
	}

	if !apply {
		confirm := kinput.Input(kinput.Blue, fmt.Sprintf("  Apply these %d statement(s) to '%s'? (y/N): ", totalSQL, dbName))
		if strings.ToLower(strings.TrimSpace(confirm)) == "y" || strings.ToLower(strings.TrimSpace(confirm)) == "yes" {
			apply = true
		}
	}

	if apply {
		fmt.Printf("\n  \033[0;33mApplying migrations to '%s'...\033[0m\n", dbName)
		n, err := ApplySchemaDiff(diffs, dbName)
		if err != nil {
			fmt.Printf("  \033[1;31m✖ Error applying migrations: %v\033[0m\n\n", err)
			return
		}
		fmt.Printf("  \033[1;32m✔ Successfully applied %d migration statement(s) to '%s'!\033[0m\n\n", n, dbName)
	}
}
