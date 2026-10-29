package korm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
	"github.com/kamalshkeir/ulid"
)

// MCP JSON-RPC 2.0 Types
type MCPRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type MCPResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *MCPError `json:"error,omitempty"`
}

type MCPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type MCPToolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type MCPToolResult struct {
	Content []MCPToolContent `json:"content"`
	IsError bool             `json:"isError,omitempty"`
}

// Available MCP Tools
func getMCPToolsList() []map[string]any {
	return []map[string]any{
		{
			"name":        "korm_get_schema",
			"description": "Returns the complete database schema and foreign key relation graph (belongs_to, has_many, unique).",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"database": map[string]any{
						"type":        "string",
						"description": "Database name (optional, defaults to connected database)",
					},
				},
			},
		},
		{
			"name":        "korm_execute_query",
			"description": "Executes a safe read-only SQL query (SELECT, WITH, EXPLAIN) with automatic LIMIT enforcement.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{
						"type":        "string",
						"description": "The SQL read-only query to execute",
					},
					"database": map[string]any{
						"type":        "string",
						"description": "Database name (optional)",
					},
					"limit": map[string]any{
						"type":        "integer",
						"description": "Maximum number of rows to return (default 50, max 200)",
					},
				},
				"required": []string{"query"},
			},
		},
		{
			"name":        "korm_get_diff",
			"description": "Inspects structural differences between declared Go structs and physical database schema.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"database": map[string]any{
						"type":        "string",
						"description": "Database name (optional)",
					},
				},
			},
		},
		{
			"name":        "korm_cache_stats",
			"description": "Returns in-memory cache statistics, entry counts, and allocated memory limits.",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "korm_generate_go_struct",
			"description": "Generates a Go struct definition with KORM tags (pk, fk, size) for an existing table.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"table": map[string]any{
						"type":        "string",
						"description": "Target table name",
					},
					"database": map[string]any{
						"type":        "string",
						"description": "Database name (optional)",
					},
				},
				"required": []string{"table"},
			},
		},
	}
}

// HandleMCPRequest dispatches any incoming JSON-RPC 2.0 MCP request.
func HandleMCPRequest(req *MCPRequest) *MCPResponse {
	resp := &MCPResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
	}

	switch req.Method {
	case "initialize":
		resp.Result = map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]any{
				"tools": map[string]any{
					"listChanged": false,
				},
			},
			"serverInfo": map[string]any{
				"name":    "korm-mcp",
				"version": "1.0.0",
			},
		}

	case "notifications/initialized":
		return nil

	case "ping":
		resp.Result = map[string]any{}

	case "tools/list":
		resp.Result = map[string]any{
			"tools": getMCPToolsList(),
		}

	case "tools/call":
		var callParams struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &callParams); err != nil {
			resp.Error = &MCPError{Code: -32602, Message: "Invalid parameters"}
			return resp
		}

		dbName := ""
		if v, ok := callParams.Arguments["database"].(string); ok {
			dbName = v
		}

		switch callParams.Name {
		case "korm_get_schema":
			res, err := MCPGetSchema(dbName)
			if err != nil {
				resp.Result = formatToolError(err)
			} else {
				resp.Result = formatToolResult(res)
			}

		case "korm_execute_query":
			q, _ := callParams.Arguments["query"].(string)
			limit := 50
			if v, ok := callParams.Arguments["limit"].(float64); ok {
				limit = int(v)
			}
			res, err := MCPExecuteReadOnlyQuery(q, dbName, limit)
			if err != nil {
				resp.Result = formatToolError(err)
			} else {
				resp.Result = formatToolResult(res)
			}

		case "korm_get_diff":
			res, err := MCPGetDiff(dbName)
			if err != nil {
				resp.Result = formatToolError(err)
			} else {
				resp.Result = formatToolResult(res)
			}

		case "korm_cache_stats":
			res := MCPGetCacheStats()
			resp.Result = formatToolResult(res)

		case "korm_generate_go_struct":
			tbl, _ := callParams.Arguments["table"].(string)
			if tbl == "" {
				resp.Result = formatToolError(fmt.Errorf("missing required 'table' argument"))
			} else {
				code, err := MCPGenerateGoStruct(tbl, dbName)
				if err != nil {
					resp.Result = formatToolError(err)
				} else {
					resp.Result = MCPToolResult{
						Content: []MCPToolContent{
							{Type: "text", Text: code},
						},
					}
				}
			}

		default:
			resp.Error = &MCPError{Code: -32601, Message: fmt.Sprintf("Unknown tool: %s", callParams.Name)}
		}

	default:
		resp.Error = &MCPError{Code: -32601, Message: fmt.Sprintf("Method not found: %s", req.Method)}
	}

	return resp
}

func formatToolResult(data any) MCPToolResult {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return MCPToolResult{
			Content: []MCPToolContent{{Type: "text", Text: fmt.Sprintf("%v", data)}},
		}
	}
	return MCPToolResult{
		Content: []MCPToolContent{{Type: "text", Text: string(b)}},
	}
}

func formatToolError(err error) MCPToolResult {
	return MCPToolResult{
		Content: []MCPToolContent{{Type: "text", Text: fmt.Sprintf("Error: %s", err.Error())}},
		IsError: true,
	}
}

// ServeMCPStdio runs the standard I/O transport loop for external IDE agents (Cursor, Claude, etc.).
func ServeMCPStdio() {
	scanner := bufio.NewScanner(os.Stdin)
	// Allow larger payloads up to 10MB
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req MCPRequest
		if err := json.Unmarshal(line, &req); err != nil {
			errResp := MCPResponse{
				JSONRPC: "2.0",
				Error:   &MCPError{Code: -32700, Message: "Parse error"},
			}
			b, _ := json.Marshal(errResp)
			fmt.Printf("%s\n", b)
			continue
		}

		resp := HandleMCPRequest(&req)
		if resp != nil {
			b, err := json.Marshal(resp)
			if err == nil {
				fmt.Printf("%s\n", b)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		lg.ErrorC("MCP stdio scanner error", "err", err)
	}
}

// SSE Transport Management for ksmux
var (
	mcpSessionsMu sync.RWMutex
	mcpSessions   = map[string]chan []byte{}
)

// MCPSSEHandler establishes an MCP SSE connection using ksmux context.
var MCPSSEHandler = func(c *ksmux.Context) {
	sessionID := ulid.Make().String()
	ch := make(chan []byte, 32)

	mcpSessionsMu.Lock()
	mcpSessions[sessionID] = ch
	mcpSessionsMu.Unlock()

	defer func() {
		mcpSessionsMu.Lock()
		delete(mcpSessions, sessionID)
		mcpSessionsMu.Unlock()
		close(ch)
	}()

	c.AddSSEHeaders()

	// Initial MCP endpoint discovery event as per spec
	endpointURL := fmt.Sprintf("/admin/mcp/message?session_id=%s", sessionID)
	_ = c.Stream(fmt.Sprintf("event: endpoint\ndata: %s", endpointURL))

	notify := c.Request.Context().Done()
	for {
		select {
		case <-notify:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			err := c.Stream(fmt.Sprintf("event: message\ndata: %s", string(msg)))
			if err != nil {
				return
			}
		}
	}
}

// MCPMessageHandler receives POST requests targeting an active SSE session.
var MCPMessageHandler = func(c *ksmux.Context) {
	sessionID := c.Request.URL.Query().Get("session_id")
	if sessionID == "" {
		c.Status(400).Json(map[string]string{"error": "missing session_id"})
		return
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 {
		c.Status(400).Json(map[string]string{"error": "empty body"})
		return
	}

	var req MCPRequest
	if err := json.Unmarshal(body, &req); err != nil {
		c.Status(400).Json(map[string]string{"error": "invalid json"})
		return
	}

	resp := HandleMCPRequest(&req)
	if resp != nil {
		respBytes, _ := json.Marshal(resp)
		mcpSessionsMu.RLock()
		ch, ok := mcpSessions[sessionID]
		mcpSessionsMu.RUnlock()
		if ok {
			ch <- respBytes
		}
	}

	c.Status(202).Text("Accepted")
}

// MCPRPCHandler provides a direct HTTP JSON-RPC endpoint (ideal for in-browser fetch/WebLLM).
var MCPRPCHandler = func(c *ksmux.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 {
		c.Status(400).Json(map[string]string{"error": "empty request body"})
		return
	}

	var req MCPRequest
	if err := json.Unmarshal(body, &req); err != nil {
		c.Status(400).Json(&MCPResponse{
			JSONRPC: "2.0",
			Error:   &MCPError{Code: -32700, Message: "Parse error"},
		})
		return
	}

	resp := HandleMCPRequest(&req)
	if resp == nil {
		c.Status(204).Text("")
		return
	}

	c.Status(200).Json(resp)
}

// WithMCP enables programmatic start of MCP stdio or registers hooks.
func WithMCP() {
	lg.Info("KORM MCP server ready")
}
