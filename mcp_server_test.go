package korm

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMCPServerHandlers(t *testing.T) {
	// 1. Test "initialize"
	initReq := &MCPRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
	}
	initResp := HandleMCPRequest(initReq)
	if initResp == nil || initResp.Error != nil {
		t.Fatalf("expected valid initialize response, got: %v", initResp)
	}
	resMap, ok := initResp.Result.(map[string]any)
	if !ok || resMap["protocolVersion"] != "2024-11-05" {
		t.Fatalf("unexpected initialize result: %v", initResp.Result)
	}

	// 2. Test "tools/list"
	listReq := &MCPRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/list",
	}
	listResp := HandleMCPRequest(listReq)
	if listResp == nil || listResp.Error != nil {
		t.Fatalf("expected valid tools/list response, got: %v", listResp)
	}
	toolsMap, ok := listResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("invalid tools/list result structure")
	}
	toolsList, ok := toolsMap["tools"].([]map[string]any)
	if !ok || len(toolsList) != 5 {
		t.Fatalf("expected 5 tools in tools/list, got %d", len(toolsList))
	}

	// 3. Test "tools/call" -> korm_get_schema
	schemaArgs, _ := json.Marshal(map[string]any{
		"name": "korm_get_schema",
		"arguments": map[string]any{
			"database": DB_TEST_NAME,
		},
	})
	callSchemaReq := &MCPRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params:  schemaArgs,
	}
	callSchemaResp := HandleMCPRequest(callSchemaReq)
	if callSchemaResp == nil || callSchemaResp.Error != nil {
		t.Fatalf("tools/call korm_get_schema failed: %v", callSchemaResp)
	}
	toolRes, ok := callSchemaResp.Result.(MCPToolResult)
	if !ok || len(toolRes.Content) == 0 {
		t.Fatalf("unexpected tool result: %v", callSchemaResp.Result)
	}
	if !strings.Contains(toolRes.Content[0].Text, "database") {
		t.Errorf("schema response text missing 'database': %s", toolRes.Content[0].Text)
	}

	// 4. Test "tools/call" -> korm_execute_query
	queryArgs, _ := json.Marshal(map[string]any{
		"name": "korm_execute_query",
		"arguments": map[string]any{
			"query":    "SELECT id, email FROM users",
			"database": DB_TEST_NAME,
			"limit":    2,
		},
	})
	callQueryReq := &MCPRequest{
		JSONRPC: "2.0",
		ID:      4,
		Method:  "tools/call",
		Params:  queryArgs,
	}
	callQueryResp := HandleMCPRequest(callQueryReq)
	if callQueryResp == nil || callQueryResp.Error != nil {
		t.Fatalf("tools/call korm_execute_query failed: %v", callQueryResp)
	}
	queryToolRes := callQueryResp.Result.(MCPToolResult)
	if !strings.Contains(queryToolRes.Content[0].Text, "rows") {
		t.Errorf("query response text missing 'rows': %s", queryToolRes.Content[0].Text)
	}

	// 5. Test "tools/call" -> korm_generate_go_struct
	structArgs, _ := json.Marshal(map[string]any{
		"name": "korm_generate_go_struct",
		"arguments": map[string]any{
			"table":    "users",
			"database": DB_TEST_NAME,
		},
	})
	callStructReq := &MCPRequest{
		JSONRPC: "2.0",
		ID:      5,
		Method:  "tools/call",
		Params:  structArgs,
	}
	callStructResp := HandleMCPRequest(callStructReq)
	if callStructResp == nil || callStructResp.Error != nil {
		t.Fatalf("tools/call korm_generate_go_struct failed: %v", callStructResp)
	}
	structToolRes := callStructResp.Result.(MCPToolResult)
	if !strings.Contains(structToolRes.Content[0].Text, "type User struct") {
		t.Errorf("expected generated struct in content, got: %s", structToolRes.Content[0].Text)
	}

	// 6. Test Unknown tool error
	unknownToolArgs, _ := json.Marshal(map[string]any{
		"name":      "non_existent_tool",
		"arguments": map[string]any{},
	})
	unknownToolReq := &MCPRequest{
		JSONRPC: "2.0",
		ID:      6,
		Method:  "tools/call",
		Params:  unknownToolArgs,
	}
	unknownToolResp := HandleMCPRequest(unknownToolReq)
	if unknownToolResp == nil || unknownToolResp.Error == nil {
		t.Fatalf("expected error for unknown tool, got nil")
	}

	// 7. Test notifications/initialized (should return nil as per JSON-RPC notification spec)
	notifReq := &MCPRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}
	if notifResp := HandleMCPRequest(notifReq); notifResp != nil {
		t.Fatalf("expected nil response for notification, got: %v", notifResp)
	}
}

