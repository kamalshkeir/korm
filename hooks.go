package korm

import (
	"net/http"

	"github.com/kamalshkeir/kmux/ws"
	"github.com/kamalshkeir/ksbus"
)

var (
	onInsert DbHook
	onSet    DbHook
	onDelete func(database, table string, query string, args ...any) error
	onDrop   func(database, table string) error
)

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
