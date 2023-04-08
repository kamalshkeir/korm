package korm

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/kamalshkeir/kmux/ws"
	"github.com/kamalshkeir/ksbus"
)

var (
	onInsert DbHook
	onSet    DbHook
	onDelete func(database, table string, query string, args ...any) error
	onDrop   func(database, table string) error
)

type myLogAndCacheHook struct{}

func (h *myLogAndCacheHook) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	if logQueries {
		fmt.Printf("> %s %q", query, args)
		return context.WithValue(ctx, "begin", time.Now()), nil
	}
	return context.Background(), nil
}

func (h *myLogAndCacheHook) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	if logQueries {
		begin := ctx.Value("begin").(time.Time)
		fmt.Printf(". took: %s\n", time.Since(begin))
		return ctx, nil
	}
	if useCache && !strings.HasPrefix(strings.ToLower(query), "select") {
		FlushCache()
	}
	return context.Background(), nil
}

type DbHook func(database, table string, data map[string]any) error

func OnInsert(fn DbHook) {
	onInsert = fn
}

func OnSet(fn DbHook) {
	onSet = fn
}

func OnDelete(fn func(database, table string, query string, args ...any) error) {
	onDelete = fn
}

func OnDrop(fn func(database, table string) error) {
	onDrop = fn
}

// BeforeServersData handle connections and data received from another server
func BeforeServersData(fn func(data any, conn *ws.Conn)) {
	ksbus.BeforeServersData = fn
}

// BeforeDataWS handle connections and data received before upgrading websockets, useful to handle authentication
func BeforeDataWS(fn func(data map[string]any, conn *ws.Conn, originalRequest *http.Request) bool) {
	ksbus.BeforeDataWS = fn
}
