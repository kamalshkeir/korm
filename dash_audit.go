package korm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
)

// KormAudit records mutation history for any table in the database
type KormAudit struct {
	Id        uint      `korm:"pk"`
	TableName string    `korm:"size:100"`
	RecordId  string    `korm:"size:100"`
	Action    string    `korm:"size:20"` // "insert", "update", "delete", "rollback"
	OldValues string    `korm:"text"`    // JSON
	NewValues string    `korm:"text"`    // JSON
	ChangedBy string    `korm:"size:100"`
	CreatedAt time.Time `korm:"now"`
}

// AuditDiffEntry represents a single field change between old and new state
type AuditDiffEntry struct {
	Field    string `json:"field"`
	OldValue any    `json:"old_value"`
	NewValue any    `json:"new_value"`
}

// AuditItemView is the structured DTO returned to the Time-Travel UI
type AuditItemView struct {
	Id        uint             `json:"id"`
	Table     string           `json:"table"`
	RecordId  string           `json:"record_id"`
	Action    string           `json:"action"`
	ChangedBy string           `json:"changed_by"`
	CreatedAt string           `json:"created_at"`
	TimeAgo   string           `json:"time_ago"`
	Diffs     []AuditDiffEntry `json:"diffs"`
	OldValues map[string]any   `json:"old_values,omitempty"`
	NewValues map[string]any   `json:"new_values,omitempty"`
}

var (
	// AuditRetentionDays defines how many days audit logs are kept (default 30 days, 0 = unlimited)
	AuditRetentionDays = 30
	// AuditMaxEntriesPerTable defines max audit entries kept per table (default 500, 0 = unlimited)
	AuditMaxEntriesPerTable = 500

	lastPruneTime time.Time
	pruneMu       sync.Mutex
)

func maybePruneAudit(db, tbl string) {
	pruneMu.Lock()
	if time.Since(lastPruneTime) < 10*time.Minute {
		pruneMu.Unlock()
		return
	}
	lastPruneTime = time.Now()
	pruneMu.Unlock()

	go func(dbName, tableName string) {
		if AuditRetentionDays > 0 {
			_, _ = PruneAuditLogs(dbName, tableName, AuditRetentionDays)
		}
		if AuditMaxEntriesPerTable > 0 {
			oldEntries, err := Select[uint]().Database(dbName).Query(
				fmt.Sprintf("SELECT id FROM _korm_audit WHERE table_name = '%s' ORDER BY id DESC LIMIT 1 OFFSET %d", tableName, AuditMaxEntriesPerTable),
			)
			if err == nil && len(oldEntries) > 0 {
				cutoffId := oldEntries[0]
				_, _ = Table("_korm_audit").Database(dbName).Where("table_name = ? AND id < ?", tableName, cutoffId).Delete()
			}
		}
	}(db, tbl)
}

// PruneAuditLogs deletes records older than olderThanDays
func PruneAuditLogs(dbName string, tableName string, olderThanDays int) (int, error) {
	if dbName == "" {
		dbName = defaultDB
	}

	if olderThanDays <= 0 {
		olderThanDays = AuditRetentionDays
	}

	cutoff := time.Now().AddDate(0, 0, -olderThanDays)
	q := Table("_korm_audit").Database(dbName).Where("created_at < ?", cutoff)
	if tableName != "" {
		q = q.Where("table_name = ?", tableName)
	}

	return q.Delete()
}

// RecordAudit persists a change in _korm_audit table and enforces retention policy asynchronously
func RecordAudit(dbName string, tableName string, recordId string, action string, oldValues any, newValues any, changedBy string) {
	if strings.HasPrefix(tableName, "_") {
		return
	}
	if dbName == "" {
		dbName = defaultDB
	}

	var oldJson string
	if oldValues != nil {
		if b, err := json.Marshal(oldValues); err == nil {
			oldJson = string(b)
		}
	}

	var newJson string
	if newValues != nil {
		if b, err := json.Marshal(newValues); err == nil {
			newJson = string(b)
		}
	}

	if changedBy == "" {
		changedBy = "admin"
	}

	audit := KormAudit{
		TableName: tableName,
		RecordId:  recordId,
		Action:    strings.ToLower(action),
		OldValues: oldJson,
		NewValues: newJson,
		ChangedBy: changedBy,
		CreatedAt: time.Now(),
	}

	_, err := Model[KormAudit]().Database(dbName).Insert(&audit)
	if err != nil {
		lg.ErrorC("unable to record audit log", "err", err, "table", tableName, "action", action)
		return
	}

	// Automatic retention policy enforcement (throttled to at most once per 10min)
	maybePruneAudit(dbName, tableName)
}

func formatTimeAgo(t time.Time) string {
	diff := time.Since(t)
	switch {
	case diff < time.Minute:
		return "Just now"
	case diff < time.Hour:
		mins := int(diff.Minutes())
		if mins <= 1 {
			return "1 min ago"
		}
		return fmt.Sprintf("%d mins ago", mins)
	case diff < 24*time.Hour:
		hours := int(diff.Hours())
		if hours <= 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", hours)
	case diff < 48*time.Hour:
		return "Yesterday"
	default:
		return t.Format("2006-01-02 15:04")
	}
}

func computeAuditDiffs(action string, oldMap map[string]any, newMap map[string]any) []AuditDiffEntry {
	diffs := make([]AuditDiffEntry, 0)
	ignored := map[string]bool{"password": true, "pass": true}

	switch action {
	case "insert":
		for k, v := range newMap {
			if ignored[k] {
				diffs = append(diffs, AuditDiffEntry{Field: k, OldValue: nil, NewValue: "••••••••"})
			} else {
				diffs = append(diffs, AuditDiffEntry{Field: k, OldValue: nil, NewValue: v})
			}
		}
	case "delete":
		for k, v := range oldMap {
			if ignored[k] {
				diffs = append(diffs, AuditDiffEntry{Field: k, OldValue: "••••••••", NewValue: nil})
			} else {
				diffs = append(diffs, AuditDiffEntry{Field: k, OldValue: v, NewValue: nil})
			}
		}
	default: // update or rollback
		// Collect all keys
		allKeys := map[string]bool{}
		for k := range oldMap {
			allKeys[k] = true
		}
		for k := range newMap {
			allKeys[k] = true
		}

		for k := range allKeys {
			vOld := oldMap[k]
			vNew := newMap[k]
			// Normalize for comparison
			sOld := fmt.Sprintf("%v", vOld)
			sNew := fmt.Sprintf("%v", vNew)
			if sOld != sNew {
				if ignored[k] {
					diffs = append(diffs, AuditDiffEntry{Field: k, OldValue: "••••••••", NewValue: "•••••••• (changed)"})
				} else {
					diffs = append(diffs, AuditDiffEntry{Field: k, OldValue: vOld, NewValue: vNew})
				}
			}
		}
	}

	return diffs
}

func getContextUser(c *ksmux.Context) string {
	if c == nil {
		return "system"
	}
	if v, ok := c.GetKey("korm-user"); ok {
		if u, ok := v.(User); ok && u.Username != "" {
			return u.Username
		}
	}
	if u, ok := c.User(); ok {
		if s, ok := u.(string); ok && s != "" {
			return s
		}
	}
	return "admin"
}

// TableAuditGET retrieves timeline audit entries for a table or specific record
var TableAuditGET = func(c *ksmux.Context) {
	table := c.Param("table")
	if table == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{"error": "table parameter required"})
		return
	}

	dbName := defaultDB

	recordId := c.QueryParam("record_id")
	limit := 50
	if l := c.QueryParam("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	builder := Model[KormAudit]().Database(dbName).Where("table_name = ?", table)
	if recordId != "" {
		builder = builder.Where("record_id = ?", recordId)
	}

	rawAudits, err := builder.OrderBy("-id").Limit(limit).All()
	if err != nil {
		c.Status(500).Json(map[string]any{"error": err.Error(), "items": []any{}})
		return
	}

	items := make([]AuditItemView, 0, len(rawAudits))
	for _, a := range rawAudits {
		var oldMap map[string]any
		if a.OldValues != "" {
			_ = json.Unmarshal([]byte(a.OldValues), &oldMap)
		}
		var newMap map[string]any
		if a.NewValues != "" {
			_ = json.Unmarshal([]byte(a.NewValues), &newMap)
		}

		diffs := computeAuditDiffs(a.Action, oldMap, newMap)

		items = append(items, AuditItemView{
			Id:        a.Id,
			Table:     a.TableName,
			RecordId:  a.RecordId,
			Action:    a.Action,
			ChangedBy: a.ChangedBy,
			CreatedAt: a.CreatedAt.Format("2006-01-02 15:04:05"),
			TimeAgo:   formatTimeAgo(a.CreatedAt),
			Diffs:     diffs,
			OldValues: oldMap,
			NewValues: newMap,
		})
	}

	c.Json(map[string]any{
		"success": true,
		"table":   table,
		"count":   len(items),
		"items":   items,
	})
}

// AuditRollbackPOST handles 1-click reverting of any historical audit mutation
var AuditRollbackPOST = func(c *ksmux.Context) {
	body := c.BodyJson()
	if body == nil {
		c.Status(http.StatusBadRequest).Json(map[string]any{"error": "empty JSON payload"})
		return
	}

	auditIdVal, ok := body["audit_id"]
	if !ok {
		c.Status(http.StatusBadRequest).Json(map[string]any{"error": "audit_id required"})
		return
	}

	var auditId uint
	switch v := auditIdVal.(type) {
	case float64:
		auditId = uint(v)
	case int:
		auditId = uint(v)
	case string:
		if parsed, err := strconv.Atoi(v); err == nil {
			auditId = uint(parsed)
		}
	}

	if auditId == 0 {
		c.Status(http.StatusBadRequest).Json(map[string]any{"error": "valid audit_id required"})
		return
	}

	dbName := defaultDB
	if v, ok := body["database"].(string); ok && v != "" {
		dbName = v
	}

	audit, err := Model[KormAudit]().Database(dbName).Where("id = ?", auditId).One()
	if err != nil {
		c.Status(http.StatusNotFound).Json(map[string]any{"error": "audit entry not found"})
		return
	}

	t, err := GetMemoryTable(audit.TableName, dbName)
	if err != nil {
		c.Status(http.StatusNotFound).Json(map[string]any{"error": "table not found in memory"})
		return
	}

	pkKey := "id"
	if t.Pk != "" {
		pkKey = t.Pk
	}

	username := getContextUser(c)

	switch audit.Action {
	case "update", "rollback":
		var oldMap map[string]any
		if audit.OldValues == "" || json.Unmarshal([]byte(audit.OldValues), &oldMap) != nil || len(oldMap) == 0 {
			c.Status(http.StatusBadRequest).Json(map[string]any{"error": "no previous values available to rollback"})
			return
		}

		current, _ := Table(audit.TableName).Database(dbName).Where(pkKey+" = ?", audit.RecordId).One()

		setParts := make([]string, 0)
		values := make([]any, 0)
		for col, val := range oldMap {
			if col == pkKey {
				continue
			}
			setParts = append(setParts, fmt.Sprintf("%s = ?", col))
			values = append(values, val)
		}

		if len(setParts) > 0 {
			_, err := Table(audit.TableName).Database(dbName).Where(pkKey+" = ?", audit.RecordId).Set(strings.Join(setParts, ", "), values...)
			if err != nil {
				c.Status(500).Json(map[string]any{"error": "rollback failed: " + err.Error()})
				return
			}
		}

		RecordAudit(dbName, audit.TableName, audit.RecordId, "rollback", current, oldMap, username)

	case "delete":
		var oldMap map[string]any
		if audit.OldValues == "" || json.Unmarshal([]byte(audit.OldValues), &oldMap) != nil || len(oldMap) == 0 {
			c.Status(http.StatusBadRequest).Json(map[string]any{"error": "no deleted snapshot available to restore"})
			return
		}

		_, err := Table(audit.TableName).Database(dbName).Insert(oldMap)
		if err != nil {
			c.Status(500).Json(map[string]any{"error": "failed to restore deleted row: " + err.Error()})
			return
		}

		RecordAudit(dbName, audit.TableName, audit.RecordId, "rollback", nil, oldMap, username)

	case "insert":
		current, _ := Table(audit.TableName).Database(dbName).Where(pkKey+" = ?", audit.RecordId).One()
		_, err := Table(audit.TableName).Database(dbName).Where(pkKey+" = ?", audit.RecordId).Delete()
		if err != nil {
			c.Status(500).Json(map[string]any{"error": "failed to remove inserted row: " + err.Error()})
			return
		}

		RecordAudit(dbName, audit.TableName, audit.RecordId, "rollback", current, nil, username)

	default:
		c.Status(http.StatusBadRequest).Json(map[string]any{"error": "unsupported action for rollback"})
		return
	}

	c.Json(map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully rolled back %s on row #%s", audit.Action, audit.RecordId),
		"table":   audit.TableName,
		"record":  audit.RecordId,
	})
}

// TableAuditPrunePOST allows manual or scheduled purging of old audit logs
var TableAuditPrunePOST = func(c *ksmux.Context) {
	body := c.BodyJson()
	table := ""
	days := AuditRetentionDays
	if body != nil {
		if v, ok := body["table"].(string); ok {
			table = v
		}
		if v, ok := body["days"].(float64); ok && v > 0 {
			days = int(v)
		} else if v, ok := body["days"].(int); ok && v > 0 {
			days = v
		}
	}
	dbName := defaultDB
	if body != nil {
		if v, ok := body["database"].(string); ok && v != "" {
			dbName = v
		}
	}

	affected, err := PruneAuditLogs(dbName, table, days)
	if err != nil {
		c.Status(500).Json(map[string]any{"error": err.Error()})
		return
	}

	c.Json(map[string]any{
		"success": true,
		"deleted": affected,
		"days":    days,
		"table":   table,
		"message": fmt.Sprintf("Pruned %d audit logs older than %d days", affected, days),
	})
}
