package korm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
)

// Add at the top with other types
type traceContextKey string

const (
	traceEnabledKey traceContextKey = "trace_enabled"
)

// TraceData represents a single trace entry
type TraceData struct {
	Query     string        // The SQL query
	Args      []any         // Query arguments
	Database  string        // Database name
	StartTime time.Time     // When the query started
	Duration  time.Duration // How long it took
	Error     error         // Any error that occurred
}

// Tracer handles query tracing functionality
type Tracer struct {
	enabled bool
	traces  []TraceData
	mu      sync.RWMutex
	maxSize int // Maximum number of traces to keep
}

var (
	defaultTracer = &Tracer{
		enabled: false,
		traces:  make([]TraceData, 0),
		maxSize: 500, // Default to keeping last 1000 traces
	}
)

// WithTracing turns on tracing db + api
func WithTracing() {
	SetMaxDBTraces(50)
	defaultTracer.enabled = true
	// enable ksmux tracing
	ksmux.EnableTracing(nil)
}

// DisableTracing turns off query tracing
func DisableTracing() {
	defaultTracer.enabled = false
	ksmux.DisableTracing()
}

// SetMaxDBTraces sets the maximum number of traces to keep
func SetMaxDBTraces(max int) {
	defaultTracer.maxSize = max
	ksmux.SetMaxTraces(max)
}

// ClearDBTraces removes all stored traces
func ClearDBTraces() {
	defaultTracer.mu.Lock()
	defaultTracer.traces = make([]TraceData, 0)
	defaultTracer.mu.Unlock()
}

// GetDBTraces returns all stored traces
func GetDBTraces() []TraceData {
	defaultTracer.mu.RLock()
	defer defaultTracer.mu.RUnlock()
	return defaultTracer.traces
}

// addTrace adds a new trace entry
func (t *Tracer) addTrace(trace TraceData) {
	if !t.enabled || trace.Query == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Add new trace
	t.traces = append(t.traces, trace)

	// Remove oldest traces if we exceed maxSize
	if len(t.traces) > t.maxSize {
		t.traces = t.traces[len(t.traces)-t.maxSize:]
	}
}

// TraceQuery wraps a query execution with tracing
func TraceQuery(ctx context.Context, db *DatabaseEntity, query string, args ...any) (TraceData, error) {
	trace := TraceData{
		Query:     query,
		Args:      args,
		Database:  db.Name,
		StartTime: time.Now(),
	}

	// Execute the query
	var err error
	if ctx != nil {
		_, err = db.Conn.ExecContext(ctx, query, args...)
	} else {
		_, err = db.Conn.Exec(query, args...)
	}

	trace.Duration = time.Since(trace.StartTime)
	trace.Error = err

	// Add trace to storage
	defaultTracer.addTrace(trace)

	if err != nil {
		lg.ErrorC("Query failed",
			"query", query,
			"args", fmt.Sprint(args...),
			"duration", trace.Duration,
			"error", err)
	} else if logQueries {
		lg.InfoC("Query executed",
			"query", query,
			"args", fmt.Sprint(args...),
			"duration", trace.Duration)
	}

	return trace, err
}
