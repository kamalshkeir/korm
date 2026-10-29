package korm

import (
	"strings"
	"testing"
)

func TestMCPTools(t *testing.T) {
	// 1. Test MCPGetSchema
	schemaRes, err := MCPGetSchema(DB_TEST_NAME)
	if err != nil {
		t.Fatalf("MCPGetSchema failed: %v", err)
	}
	if schemaRes.Database != DB_TEST_NAME {
		t.Errorf("expected db %s, got %s", DB_TEST_NAME, schemaRes.Database)
	}
	if len(schemaRes.Tables) == 0 {
		t.Errorf("expected at least 1 table in schema, got 0")
	}

	// 2. Test MCPExecuteReadOnlyQuery (Valid query)
	qRes, err := MCPExecuteReadOnlyQuery("SELECT id, email FROM users", DB_TEST_NAME, 5)
	if err != nil {
		t.Fatalf("MCPExecuteReadOnlyQuery failed: %v", err)
	}
	if len(qRes.Rows) > 5 {
		t.Errorf("expected max 5 rows, got %d", len(qRes.Rows))
	}
	if !strings.Contains(qRes.Query, "LIMIT 5") {
		t.Errorf("expected query to enforce LIMIT 5, got: %s", qRes.Query)
	}

	// 3. Test MCPExecuteReadOnlyQuery Security Rejections
	// A: Mutation query rejection
	_, err = MCPExecuteReadOnlyQuery("DELETE FROM users WHERE id = 1", DB_TEST_NAME)
	if err == nil {
		t.Errorf("expected error for DELETE query, got nil")
	}

	// B: Multiple statements rejection
	_, err = MCPExecuteReadOnlyQuery("SELECT 1; DROP TABLE users", DB_TEST_NAME)
	if err == nil {
		t.Errorf("expected error for stacked query with semicolon, got nil")
	}

	// C: Non-SELECT start rejection
	_, err = MCPExecuteReadOnlyQuery("UPDATE users SET email = 'hacked'", DB_TEST_NAME)
	if err == nil {
		t.Errorf("expected error for UPDATE query, got nil")
	}

	// 4. Test MCPGetCacheStats
	cacheStats := MCPGetCacheStats()
	if cacheStats == nil {
		t.Fatalf("expected non-nil cacheStats")
	}
	if cacheStats.MaxMemoryMb <= 0 {
		t.Errorf("expected positive max memory, got %d", cacheStats.MaxMemoryMb)
	}

	// 5. Test MCPGenerateGoStruct
	goStruct, err := MCPGenerateGoStruct("users", DB_TEST_NAME)
	if err != nil {
		t.Fatalf("MCPGenerateGoStruct failed: %v", err)
	}
	if !strings.Contains(goStruct, "type User struct") {
		t.Errorf("expected 'type User struct', got:\n%s", goStruct)
	}
	if !strings.Contains(goStruct, "korm:\"pk") {
		t.Errorf("expected primary key tag in struct, got:\n%s", goStruct)
	}

	// 6. Test MCPGetDiff
	diffRes, err := MCPGetDiff(DB_TEST_NAME)
	if err != nil {
		t.Fatalf("MCPGetDiff failed: %v", err)
	}
	if diffRes == nil {
		t.Fatalf("expected non-nil diffRes")
	}
}

