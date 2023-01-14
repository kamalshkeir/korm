package korm

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
