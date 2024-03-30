package korm

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
)

var (
	onInsert DbHook
	onSet    DbHook
	onDelete func(database, table string, query string, args ...any) error
	onDrop   func(database, table string) error
)

type logAndCacheHook struct{}

func (h *logAndCacheHook) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	if useCache && len(query) > 6 {
		if !onceDone {
			go RunEvery(FlushCacheEvery, func(cancelChan chan struct{}) {
				if !useCache {
					cancelChan <- struct{}{}
				}
				flushCache()
			})
			onceDone = true
		}
		q := strings.TrimSpace(strings.ToLower(query[:6]))
		if q != "select" && q != "pragma" {
			flushCache()
		}
	}
	if logQueries {
		lg.Printfs("yl> %s %v", query, args)
		return context.WithValue(ctx, ksmux.ContextKey("begin"), time.Now()), nil
	}
	return context.Background(), nil
}

func (h *logAndCacheHook) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	if logQueries {
		begin := ctx.Value(ksmux.ContextKey("begin")).(time.Time)
		lg.Printfs("yl, took: %v\n", time.Since(begin))
		return ctx, nil
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

// Hook is the hook callback signature
type Hook func(ctx context.Context, query string, args ...interface{}) (context.Context, error)

// ErrorHook is the error handling callback signature
type ErrorHook func(ctx context.Context, err error, query string, args ...interface{}) error

// Hooks instances may be passed to Wrap() to define an instrumented driver
type Hooks interface {
	Before(ctx context.Context, query string, args ...interface{}) (context.Context, error)
	After(ctx context.Context, query string, args ...interface{}) (context.Context, error)
}

// OnErrorer instances will be called if any error happens
type OnErrorer interface {
	OnError(ctx context.Context, err error, query string, args ...interface{}) error
}

func handlerErr(ctx context.Context, hooks Hooks, err error, query string, args ...interface{}) error {
	h, ok := hooks.(OnErrorer)
	if !ok {
		return err
	}

	if err := h.OnError(ctx, err, query, args...); err != nil {
		return err
	}

	return err
}

// Driver implements a database/sql/driver.Driver
type Driver struct {
	driver.Driver
	hooks Hooks
}

// Open opens a connection
func (drv *Driver) Open(name string) (driver.Conn, error) {
	conn, err := drv.Driver.Open(name)
	if err != nil {
		return conn, err
	}

	// Drivers that don't implement driver.ConnBeginTx are not supported.
	if _, ok := conn.(driver.ConnBeginTx); !ok {
		return nil, errors.New("driver must implement driver.ConnBeginTx")
	}

	wrapped := &driverConn{conn, drv.hooks}
	if isExecer(conn) && isQueryer(conn) && isSessionResetter(conn) {
		return &ExecerQueryerContextWithSessionResetter{wrapped,
			&execerContext{wrapped}, &queryerContext{wrapped},
			&SessionResetter{wrapped}}, nil
	} else if isExecer(conn) && isQueryer(conn) {
		return &ExecerQueryerContext{wrapped, &execerContext{wrapped},
			&queryerContext{wrapped}}, nil
	} else if isExecer(conn) {
		// If conn implements an Execer interface, return a driver.Conn which
		// also implements Execer
		return &execerContext{wrapped}, nil
	} else if isQueryer(conn) {
		// If conn implements an Queryer interface, return a driver.Conn which
		// also implements Queryer
		return &queryerContext{wrapped}, nil
	}
	return wrapped, nil
}

// driverConn implements a database/sql.driver.driverConn
type driverConn struct {
	Conn  driver.Conn
	hooks Hooks
}

func isSessionResetter(conn driver.Conn) bool {
	_, ok := conn.(driver.SessionResetter)
	return ok
}

func (conn *driverConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var (
		stmt driver.Stmt
		err  error
	)

	if c, ok := conn.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = c.PrepareContext(ctx, query)
	} else {
		stmt, err = conn.Prepare(query)
	}

	if err != nil {
		return stmt, err
	}

	return &Stmt{stmt, conn.hooks, query}, nil
}

func (conn *driverConn) Prepare(query string) (driver.Stmt, error) {
	return conn.Conn.Prepare(query)
}
func (conn *driverConn) Close() error { return conn.Conn.Close() }
func (conn *driverConn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}
func (conn *driverConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return conn.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

// execerContext implements a database/sql.driver.execerContext
type execerContext struct {
	*driverConn
}

func isExecer(conn driver.Conn) bool {
	switch conn.(type) {
	case driver.ExecerContext:
		return true
	default:
		return false
	}
}

func (conn *execerContext) execContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch c := conn.Conn.(type) {
	case driver.ExecerContext:
		return c.ExecContext(ctx, query, args)
	default:
		// This should not happen
		return nil, errors.New("ExecerContext created for a non Execer driver.Conn")
	}
}

func (conn *execerContext) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var err error

	list := namedValueToAny(args)

	// Exec `Before` Hooks
	if ctx, err = conn.hooks.Before(ctx, query, list...); err != nil {
		return nil, err
	}

	results, err := conn.execContext(ctx, query, args)
	if err != nil {
		return results, handlerErr(ctx, conn.hooks, err, query, list...)
	}

	if _, err := conn.hooks.After(ctx, query, list...); err != nil {
		return nil, err
	}

	return results, err
}

func (conn *execerContext) Exec(query string, args []driver.Value) (driver.Result, error) {
	// We have to implement Exec since it is required in the current version of
	// Go for it to run ExecContext. From Go 10 it will be optional. However,
	// this code should never run since database/sql always prefers to run
	// ExecContext.
	return nil, errors.New("Exec was called when ExecContext was implemented")
}

// queryerContext implements a database/sql.driver.queryerContext
type queryerContext struct {
	*driverConn
}

func isQueryer(conn driver.Conn) bool {
	switch conn.(type) {
	case driver.QueryerContext:
		return true
	default:
		return false
	}
}

func (conn *queryerContext) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch c := conn.Conn.(type) {
	case driver.QueryerContext:
		return c.QueryContext(ctx, query, args)
	default:
		// This should not happen
		return nil, errors.New("QueryerContext created for a non Queryer driver.Conn")
	}
}

func (conn *queryerContext) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var err error

	list := namedValueToAny(args)

	// Query `Before` Hooks
	if ctx, err = conn.hooks.Before(ctx, query, list...); err != nil {
		return nil, err
	}

	results, err := conn.queryContext(ctx, query, args)
	if err != nil {
		return results, handlerErr(ctx, conn.hooks, err, query, list...)
	}

	if _, err := conn.hooks.After(ctx, query, list...); err != nil {
		return nil, err
	}

	return results, err
}

// ExecerQueryerContext implements database/sql.driver.ExecerContext and
// database/sql.driver.QueryerContext
type ExecerQueryerContext struct {
	*driverConn
	*execerContext
	*queryerContext
}

// ExecerQueryerContext implements database/sql.driver.ExecerContext and
// database/sql.driver.QueryerContext
type ExecerQueryerContextWithSessionResetter struct {
	*driverConn
	*execerContext
	*queryerContext
	*SessionResetter
}

type SessionResetter struct {
	*driverConn
}

// Stmt implements a database/sql/driver.Stmt
type Stmt struct {
	Stmt  driver.Stmt
	hooks Hooks
	query string
}

func (stmt *Stmt) execContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s, ok := stmt.Stmt.(driver.StmtExecContext); ok {
		return s.ExecContext(ctx, args)
	}

	values := make([]driver.Value, len(args))
	for _, arg := range args {
		values[arg.Ordinal-1] = arg.Value
	}

	return stmt.Exec(values)
}

func (stmt *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	var err error

	list := namedValueToAny(args)

	// Exec `Before` Hooks
	if ctx, err = stmt.hooks.Before(ctx, stmt.query, list...); err != nil {
		return nil, err
	}

	results, err := stmt.execContext(ctx, args)
	if err != nil {
		return results, handlerErr(ctx, stmt.hooks, err, stmt.query, list...)
	}

	if _, err := stmt.hooks.After(ctx, stmt.query, list...); err != nil {
		return nil, err
	}

	return results, err
}

func (stmt *Stmt) queryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s, ok := stmt.Stmt.(driver.StmtQueryContext); ok {
		return s.QueryContext(ctx, args)
	}

	values := make([]driver.Value, len(args))
	for _, arg := range args {
		values[arg.Ordinal-1] = arg.Value
	}
	return stmt.Query(values)
}

func (stmt *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var err error

	list := namedValueToAny(args)

	// Exec Before Hooks
	if ctx, err = stmt.hooks.Before(ctx, stmt.query, list...); err != nil {
		return nil, err
	}

	rows, err := stmt.queryContext(ctx, args)
	if err != nil {
		return rows, handlerErr(ctx, stmt.hooks, err, stmt.query, list...)
	}

	if _, err := stmt.hooks.After(ctx, stmt.query, list...); err != nil {
		return nil, err
	}

	return rows, err
}

func (stmt *Stmt) Close() error  { return stmt.Stmt.Close() }
func (stmt *Stmt) NumInput() int { return stmt.Stmt.NumInput() }
func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, 0, len(args))
	for i, a := range args {
		v := driver.NamedValue{
			Ordinal: i + 1,
			Name:    "",
			Value:   a,
		}
		named = append(named, v)
	}
	return stmt.ExecContext(context.Background(), named)
}
func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, 0, len(args))
	for i, a := range args {
		v := driver.NamedValue{
			Ordinal: i + 1,
			Name:    "",
			Value:   a,
		}
		named = append(named, v)
	}
	return stmt.QueryContext(context.Background(), named)
}

func Wrap(driver driver.Driver, hooks Hooks) driver.Driver {
	return &Driver{driver, hooks}
}

func WrapConn(conn driver.Conn, hooks Hooks) driver.Conn {
	return &driverConn{conn, hooks}
}

func namedValueToAny(args []driver.NamedValue) []interface{} {
	list := make([]interface{}, len(args))
	for i, a := range args {
		list[i] = a.Value
	}
	return list
}
