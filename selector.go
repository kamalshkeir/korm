package korm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/kamalshkeir/kstrct"
	"github.com/kamalshkeir/lg"
)

// adaptAsSingleToDoubleQuotes replaces AS 'alias' with AS "alias" for PostgreSQL compatibility
var asQuoteRegex = regexp.MustCompile(`(?i)\bAS\s+'([^']*)'`)

func adaptAsSingleToDoubleQuotes(statement string) string {
	return asQuoteRegex.ReplaceAllString(statement, `AS "$1"`)
}

type Selector[T any] struct {
	nested  bool
	debug   bool
	ctx     context.Context
	db      *DatabaseEntity
	dest    *[]T
	nocache bool
	trace   bool
}

func Select[T any](nestedSlice ...bool) *Selector[T] {
	var dest []T
	s := &Selector[T]{
		dest: &dest,
		db:   &databases[0],
	}
	if len(nestedSlice) > 0 && nestedSlice[0] {
		s.nested = true
	}
	return s
}

func (sl *Selector[T]) Database(dbName string) *Selector[T] {
	db, err := GetMemoryDatabase(dbName)
	if err == nil {
		sl.db = db
	} else if sl.db == nil {
		sl.db = &databases[0]
	}
	return sl
}

func (sl *Selector[T]) Ctx(ct context.Context) *Selector[T] {
	sl.ctx = ct
	return sl
}

func (sl *Selector[T]) Debug() *Selector[T] {
	sl.debug = true
	return sl
}

func (sl *Selector[T]) NoCache() *Selector[T] {
	sl.nocache = true
	return sl
}

func (sl *Selector[T]) Trace() *Selector[T] {
	if sl == nil {
		return nil
	}
	sl.trace = true
	if sl.ctx == nil {
		sl.ctx = context.Background()
	}
	sl.ctx = context.WithValue(sl.ctx, traceEnabledKey, true)
	return sl
}

func (sl *Selector[T]) Channel(ch T) *Selector[T] {
	if sl == nil || sl.dest == nil {
		return sl
	}
	*sl.dest = []T{ch}
	return sl
}

func (sl *Selector[T]) Query(statement string, args ...any) ([]T, error) {
	if sl == nil || sl.dest == nil {
		return nil, errors.New("selector or destination is nil")
	}
	if sl.db == nil {
		if len(databases) > 0 {
			sl.db = &databases[0]
		} else {
			return nil, errors.New("no database connection available")
		}
	}
	// Ensure we have a context with trace flag if tracing is enabled
	if sl.trace && sl.ctx == nil {
		sl.ctx = context.WithValue(context.Background(), traceEnabledKey, true)
	}

	var stt string
	if useCache && !sl.nocache {
		// Include database name in cache key to prevent cross-database cache pollution
		stt = sl.db.Name + "::" + statement + fmt.Sprint(args...)
		if v, ok := cacheQ.Get(stt); ok {
			if len(*sl.dest) == 0 {
				*sl.dest = v.([]T)
				return *sl.dest, nil
			}
		}
	}

	typ := fmt.Sprintf("%T", *new(T))
	ref := reflect.ValueOf(*new(T))

	// Expand IN clause arguments before dialect adaptation
	statement, args = In(statement, args...)
	AdaptPlaceholdersToDialect(&statement, sl.db.Dialect)
	adaptTimeToUnixArgs(&args)
	// Replace AS 'alias' with AS "alias" for PostgreSQL compatibility
	statement = adaptAsSingleToDoubleQuotes(statement)
	var rows *sql.Rows
	var err error
	if sl.debug {
		lg.Info("DEBUG SELECTOR", "statement", statement, "args", fmt.Sprintf("%v", args))
	}

	if sl.ctx != nil {
		rows, err = sl.db.Conn.QueryContext(sl.ctx, statement, args...)
	} else {
		rows, err = sl.db.Conn.Query(statement, args...)
	}
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	isMap, isChan, isStrct, isArith, isPtr := false, false, false, false, false
	if typ[0] == '*' {
		isPtr = true
		typ = typ[1:]
	}
	isNested := sl.nested
	if len(*sl.dest) == 1 || len(typ) >= 4 && typ[:4] == "chan" {
		isChan = true
		if strings.Contains(typ, "map[string]") {
			isMap = true
		}
	}
	if typ[:3] == "map" {
		isMap = true
	} else if isNested || strings.Contains(typ, ".") || ref.Kind() == reflect.Struct || (ref.Kind() == reflect.Chan && ref.Type().Elem().Kind() == reflect.Struct) {
		if strings.HasSuffix(typ, "Time") {
			isArith = true
		} else {
			isStrct = true
		}
	} else {
		isArith = true
	}
	var (
		columns_ptr_to_values = make([]any, len(columns))
		kv                    = make([]kstrct.KV, len(columns))
		temp                  = new(T)
		lastData              []kstrct.KV
	)
	index := 0
	defer rows.Close()
loop:
	for rows.Next() {
		for i := range kv {
			kv[i].Key = columns[i]
			columns_ptr_to_values[i] = &kv[i].Value
		}
		err := rows.Scan(columns_ptr_to_values...)
		if err != nil {
			return nil, err
		}
		// Convert []byte to string for all dialects (MySQL, SQLite, PostgreSQL can all return []byte for certain column types)
		for i, kvv := range kv {
			if v, ok := kvv.Value.([]byte); ok {
				kv[i] = kstrct.KV{Key: kvv.Key, Value: string(v)}
			}
		}
		switch {
		case isStrct && !isChan:
			if !isNested {
				if isPtr {
					t := ref.Type().Elem()
					newElem := reflect.New(t).Interface().(T)
					err := kstrct.Fill(newElem, kv)
					if lg.CheckError(err) {
						return nil, err
					}
					*sl.dest = append(*sl.dest, newElem)
				} else {
					err := kstrct.Fill(temp, kv)
					if lg.CheckError(err) {
						return nil, err
					}
					*sl.dest = append(*sl.dest, *temp)
					ResetStruct(temp)
				}
				continue loop
			}
			if len(lastData) == 0 {
				lastData = make([]kstrct.KV, len(kv))
				copy(lastData, kv)
				if isPtr {
					t := reflect.TypeOf(*new(T)).Elem()
					newElem := reflect.New(t).Interface().(T)
					*sl.dest = append(*sl.dest, newElem)
					temp = &(*sl.dest)[0]
					err := kstrct.Fill(*temp, kv, true)
					if lg.CheckError(err) {
						return nil, err
					}
				} else {
					*sl.dest = append(*sl.dest, *new(T))
					temp = &(*sl.dest)[0]
					err := kstrct.Fill(temp, kv, true)
					if lg.CheckError(err) {
						return nil, err
					}
				}
			} else {
				for _, kvv := range kv {
					if kvv.Key == columns[0] {
						foundk := false
						for _, ld := range lastData {
							if ld.Key == columns[0] && ld.Value == kvv.Value {
								foundk = true
								break
							}
						}
						if !foundk {
							lastData = append(lastData, kvv)
							index++
							if isPtr {
								t := reflect.TypeOf(*new(T)).Elem()
								newElem := reflect.New(t).Interface().(T)
								*sl.dest = append(*sl.dest, newElem)
								temp = &(*sl.dest)[index]
								err := kstrct.Fill(*temp, kv, true)
								if lg.CheckError(err) {
									return nil, err
								}
							} else {
								*sl.dest = append(*sl.dest, *new(T))
								temp = &(*sl.dest)[index]
								err := kstrct.Fill(temp, kv, true)
								if lg.CheckError(err) {
									return nil, err
								}
							}
						} else {
							lastData = append(lastData, kvv)
							if !isPtr {
								err := kstrct.Fill(&(*sl.dest)[index], kv, true)
								if lg.CheckError(err) {
									return nil, err
								}
							} else {
								err := kstrct.Fill((*sl.dest)[index], kv, true)
								if lg.CheckError(err) {
									return nil, err
								}
							}

						}
						break
					}
				}
			}
			continue loop
		case isMap && !isChan:
			if isNested {
				return nil, fmt.Errorf("map is not nested struct")
			}
			m := make(map[string]any, len(kv))
			for _, kvv := range kv {
				m[kvv.Key] = kvv.Value
			}
			if isPtr {
				if v, ok := any(&m).(T); ok {
					*sl.dest = append(*sl.dest, v)
				}
			} else {
				if v, ok := any(m).(T); ok {
					*sl.dest = append(*sl.dest, v)
				}
			}
			continue loop
		case isArith && !isChan:
			if isNested {
				return nil, fmt.Errorf("arithmetic types cannot be nested")
			}
			if len(kv) == 1 {
				for _, vKV := range kv {
					vv := vKV.Value
					if isPtr {
						if vok, ok := any(&vv).(T); ok {
							*sl.dest = append(*sl.dest, vok)
						} else {
							elem := reflect.New(ref.Type()).Elem()
							err := kstrct.SetReflectFieldValue(elem, vKV.Value)
							if err != nil {
								return nil, err
							}
							*sl.dest = append(*sl.dest, elem.Interface().(T))
						}
					} else {
						if vok, ok := any(vv).(T); ok {
							*sl.dest = append(*sl.dest, vok)
						} else {
							elem := reflect.New(ref.Type()).Elem()
							err := kstrct.SetReflectFieldValue(elem, vKV.Value)
							if err != nil {
								return nil, err
							}
							*sl.dest = append(*sl.dest, elem.Interface().(T))
						}
					}
				}
				continue loop
			}
		case isChan:
			switch {
			case isStrct:
				if isPtr {
					return nil, fmt.Errorf("channel of pointers not allowed in case of structs")
				}
				if !isNested {
					err := kstrct.Fill((*sl.dest)[0], kv)
					if lg.CheckError(err) {
						return nil, err
					}
					continue loop
				} else {
					err := kstrct.Fill((*sl.dest)[0], kv, true)
					if lg.CheckError(err) {
						return nil, err
					}
					continue loop
				}

			case isMap:
				if isNested {
					return nil, fmt.Errorf("map cannot be nested")
				}
				m := make(map[string]any, len(kv))
				for _, vkv := range kv {
					m[vkv.Key] = vkv.Value
				}
				if v, ok := any((*sl.dest)[0]).(chan map[string]any); ok {
					v <- m
				} else if v, ok := any((*sl.dest)[0]).(chan *map[string]any); ok {
					v <- &m
				}
				continue loop
			case isArith:
				if isNested {
					return nil, fmt.Errorf("type cannot be nested")
				}
				chanType := reflect.New(ref.Type().Elem()).Elem()
				for _, vKv := range kv {
					if chanType.Kind() == reflect.Struct || (chanType.Kind() == reflect.Ptr && chanType.Elem().Kind() == reflect.Struct) {
						m := make(map[string]any, len(kv))
						for _, vkv := range kv {
							m[vkv.Key] = vkv.Value
						}
						err := kstrct.SetReflectFieldValue(chanType, m)
						if lg.CheckError(err) {
							return nil, err
						}
					} else {
						err := kstrct.SetReflectFieldValue(chanType, vKv.Value)
						if lg.CheckError(err) {
							return nil, err
						}
					}
					reflect.ValueOf((*sl.dest)[0]).Send(chanType)
				}
			default:
				return nil, fmt.Errorf("channel case not handled")
			}
		default:
			return nil, fmt.Errorf("default triggered, case not handled")
		}
	}
	if useCache && !sl.nocache && !isChan && len(*sl.dest) > 0 {
		cacheQ.Set(stt, *sl.dest)
	}
	return *sl.dest, nil
}

func (sl *Selector[T]) Named(statement string, args map[string]any, unsafe ...bool) ([]T, error) {
	if sl == nil || sl.dest == nil {
		return nil, errors.New("selector or destination is nil")
	}
	if sl.db == nil {
		if len(databases) > 0 {
			sl.db = &databases[0]
		} else {
			return nil, errors.New("no database connection available")
		}
	}
	if sl.trace {
		if sl.ctx == nil {
			sl.ctx = context.Background()
		}
		sl.ctx = context.WithValue(sl.ctx, traceEnabledKey, true)
	}

	var stt string
	if useCache && !sl.nocache {
		// Include database name in cache key to prevent cross-database cache pollution
		stt = sl.db.Name + "::" + statement + fmt.Sprint(args)
		if v, ok := cacheQ.Get(stt); ok {
			if len(*sl.dest) == 0 {
				*sl.dest = v.([]T)
				return *sl.dest, nil
			}
		}
	}

	typ := fmt.Sprintf("%T", *new(T))
	ref := reflect.ValueOf(sl.dest)

	for k, vv := range args {
		switch v := vv.(type) {
		case time.Time:
			args[k] = v.Unix()
		case *time.Time:
			if v != nil {
				args[k] = v.Unix()
			} else {
				delete(args, k)
				continue
			}
		case string:
			args[k] = strings.ReplaceAll(v, "T", " ")
		case *string:
			if v != nil {
				args[k] = strings.ReplaceAll(*v, "T", " ")
			} else {
				delete(args, k)
				continue
			}
		}
	}
	var query string
	var newargs []any
	if len(unsafe) > 0 && unsafe[0] {
		var err error
		query, err = UnsafeNamedQuery(statement, args)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		query, newargs, err = AdaptNamedParams(sl.db.Dialect, statement, args)
		if err != nil {
			return nil, err
		}
	}
	// Replace AS 'alias' with AS "alias" for PostgreSQL compatibility
	query = adaptAsSingleToDoubleQuotes(query)
	var rows *sql.Rows
	var err error
	if sl.debug {
		lg.Printfs("yl%s , args: %v", query, newargs)
	}
	if sl.ctx != nil {
		rows, err = sl.db.Conn.QueryContext(sl.ctx, query, newargs...)
	} else {
		rows, err = sl.db.Conn.Query(query, newargs...)
	}
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	isMap, isChan, isStrct, isArith, isPtr := false, false, false, false, false
	if typ[0] == '*' {
		isPtr = true
		typ = typ[1:]
	}
	isNested := sl.nested
	if len(*sl.dest) == 1 || len(typ) >= 4 && typ[:4] == "chan" {
		isChan = true
		if strings.Contains(typ, "map[string]") {
			isMap = true
		}
	}
	if typ[:3] == "map" {
		isMap = true
	} else if isNested || strings.Contains(typ, ".") || ref.Kind() == reflect.Struct || (ref.Kind() == reflect.Chan && ref.Type().Elem().Kind() == reflect.Struct) {
		if strings.HasSuffix(typ, "Time") {
			isArith = true
		} else {
			isStrct = true
		}
	} else {
		isArith = true
	}
	var (
		columns_ptr_to_values = make([]any, len(columns))
		kv                    = make([]kstrct.KV, len(columns))
		temp                  = new(T)
		lastData              []kstrct.KV
	)
	index := 0
	defer rows.Close()
loop:
	for rows.Next() {
		for i := range kv {
			kv[i].Key = columns[i]
			columns_ptr_to_values[i] = &kv[i].Value
		}
		err := rows.Scan(columns_ptr_to_values...)
		if err != nil {
			return nil, err
		}
		// Convert []byte to string for all dialects (MySQL, SQLite, PostgreSQL can all return []byte for certain column types)
		for i, kvv := range kv {
			if v, ok := kvv.Value.([]byte); ok {
				kv[i] = kstrct.KV{Key: kvv.Key, Value: string(v)}
			}
		}
		switch {
		case isStrct && !isChan:
			if !isNested {
				if isPtr {
					t := ref.Type().Elem()
					newElem := reflect.New(t).Interface().(T)
					err := kstrct.Fill(newElem, kv)
					if lg.CheckError(err) {
						return nil, err
					}
					*sl.dest = append(*sl.dest, newElem)
				} else {
					err := kstrct.Fill(temp, kv)
					if lg.CheckError(err) {
						return nil, err
					}
					*sl.dest = append(*sl.dest, *temp)
					ResetStruct(temp)
				}
				continue loop
			}
			if len(lastData) == 0 {
				lastData = make([]kstrct.KV, len(kv))
				copy(lastData, kv)
				if isPtr {
					t := reflect.TypeOf(*new(T)).Elem()
					newElem := reflect.New(t).Interface().(T)
					*sl.dest = append(*sl.dest, newElem)
					temp = &(*sl.dest)[0]
					err := kstrct.Fill(*temp, kv, true)
					if lg.CheckError(err) {
						return nil, err
					}
				} else {
					*sl.dest = append(*sl.dest, *new(T))
					temp = &(*sl.dest)[0]
					err := kstrct.Fill(temp, kv, true)
					if lg.CheckError(err) {
						return nil, err
					}
				}
			} else {
				for _, kvv := range kv {
					if kvv.Key == columns[0] {
						foundk := false
						for _, ld := range lastData {
							if ld.Key == columns[0] && ld.Value == kvv.Value {
								foundk = true
								break
							}
						}
						if !foundk {
							lastData = append(lastData, kvv)
							index++
							if isPtr {
								t := reflect.TypeOf(*new(T)).Elem()
								newElem := reflect.New(t).Interface().(T)
								*sl.dest = append(*sl.dest, newElem)
								temp = &(*sl.dest)[index]
								err := kstrct.Fill(*temp, kv, true)
								if lg.CheckError(err) {
									return nil, err
								}
							} else {
								*sl.dest = append(*sl.dest, *new(T))
								temp = &(*sl.dest)[index]
								err := kstrct.Fill(temp, kv, true)
								if lg.CheckError(err) {
									return nil, err
								}
							}
						} else {
							lastData = append(lastData, kvv)
							if !isPtr {
								err := kstrct.Fill(&(*sl.dest)[index], kv, true)
								if lg.CheckError(err) {
									return nil, err
								}
							} else {
								err := kstrct.Fill((*sl.dest)[index], kv, true)
								if lg.CheckError(err) {
									return nil, err
								}
							}

						}
						break
					}
				}
			}
			continue loop
		case isMap && !isChan:
			if isNested {
				return nil, fmt.Errorf("map is not nested struct")
			}
			m := make(map[string]any, len(kv))
			for _, kvv := range kv {
				m[kvv.Key] = kvv.Value
			}
			if isPtr {
				if v, ok := any(&m).(T); ok {
					*sl.dest = append(*sl.dest, v)
				}
			} else {
				if v, ok := any(m).(T); ok {
					*sl.dest = append(*sl.dest, v)
				}
			}
			continue loop
		case isArith && !isChan:
			if isNested {
				return nil, fmt.Errorf("this type cannot be nested")
			}
			if len(kv) == 1 {
				for _, vKV := range kv {
					vv := vKV.Value
					if isPtr {
						if vok, ok := any(&vv).(T); ok {
							*sl.dest = append(*sl.dest, vok)
						} else {
							elem := reflect.New(ref.Type()).Elem()
							err := kstrct.SetReflectFieldValue(elem, vKV.Value)
							if err != nil {
								return nil, err
							}
							*sl.dest = append(*sl.dest, elem.Interface().(T))
						}
					} else {
						if vok, ok := any(vv).(T); ok {
							*sl.dest = append(*sl.dest, vok)
						} else {
							elem := reflect.New(ref.Type()).Elem()
							err := kstrct.SetReflectFieldValue(elem, vKV.Value)
							if err != nil {
								return nil, err
							}
							inter := elem.Interface()
							*sl.dest = append(*sl.dest, inter.(T))
						}
					}
				}
				continue loop
			}
		case isChan:
			switch {
			case isStrct:
				if isPtr {
					return nil, fmt.Errorf("channel of pointers not allowed in case of structs")
				}
				if !isNested {
					err := kstrct.Fill((*sl.dest)[0], kv)
					if lg.CheckError(err) {
						return nil, err
					}
					continue loop
				} else {
					err := kstrct.Fill((*sl.dest)[0], kv, true)
					if lg.CheckError(err) {
						return nil, err
					}
					continue loop
				}
			case isMap:
				if isNested {
					return nil, fmt.Errorf("map cannot be nested")
				}
				m := make(map[string]any, len(kv))
				for _, vkv := range kv {
					m[vkv.Key] = vkv.Value
				}
				if v, ok := any((*sl.dest)[0]).(chan map[string]any); ok {
					v <- m
				} else if v, ok := any((*sl.dest)[0]).(chan *map[string]any); ok {
					v <- &m
				}
				continue loop
			case isArith:
				if isNested {
					return nil, fmt.Errorf("type cannot be nested")
				}
				chanType := reflect.New(ref.Type().Elem()).Elem()
				for _, vKv := range kv {
					if chanType.Kind() == reflect.Struct || (chanType.Kind() == reflect.Ptr && chanType.Elem().Kind() == reflect.Struct) {
						m := make(map[string]any, len(kv))
						for _, vkv := range kv {
							m[vkv.Key] = vkv.Value
						}
						err := kstrct.SetReflectFieldValue(chanType, m)
						if lg.CheckError(err) {
							return nil, err
						}
					} else {
						err := kstrct.SetReflectFieldValue(chanType, vKv.Value)
						if lg.CheckError(err) {
							return nil, err
						}
					}
					reflect.ValueOf((*sl.dest)[0]).Send(chanType)
				}
			default:
				return nil, fmt.Errorf("channel case not handled")
			}
		default:
			return nil, fmt.Errorf("default triggered, case not handled")
		}
	}
	if useCache && !sl.nocache && !isChan && len(*sl.dest) > 0 {
		cacheQ.Set(stt, *sl.dest)
	}
	return *sl.dest, nil
}

// One executes the query and returns a single record of T. Returns ErrNoData if no row was found.
func (sl *Selector[T]) One(statement string, args ...any) (T, error) {
	res, err := sl.Query(statement, args...)
	if err != nil {
		return *new(T), err
	}
	if len(res) == 0 {
		return *new(T), ErrNoData
	}
	return res[0], nil
}

// NamedOne executes the named query and returns a single record of T. Returns ErrNoData if no row was found.
func (sl *Selector[T]) NamedOne(statement string, args map[string]any, unsafe ...bool) (T, error) {
	res, err := sl.Named(statement, args, unsafe...)
	if err != nil {
		return *new(T), err
	}
	if len(res) == 0 {
		return *new(T), ErrNoData
	}
	return res[0], nil
}

// The input can be a struct, a pointer to a struct, or a pointer to a pointer to a struct.
func ResetStruct(input interface{}) error {
	v := reflect.ValueOf(input)

	// Handle double pointer
	if v.Kind() == reflect.Ptr && v.Elem().Kind() == reflect.Ptr {
		if v.IsNil() || v.Elem().IsNil() || v.Elem().Elem().Kind() != reflect.Struct {
			return errors.New("input must be a non-nil pointer to a struct or a struct")
		}
		v = v.Elem().Elem() // Dereference to get to the struct
	} else if v.Kind() == reflect.Ptr {
		// Handle single pointer
		if v.IsNil() || v.Elem().Kind() != reflect.Struct {
			return errors.New("input must be a non-nil pointer to a struct or a struct")
		}
		v = v.Elem()
	} else if v.Kind() != reflect.Struct {
		// Handle non-pointer struct
		return errors.New("input must be a struct or a pointer to a struct")
	}

	// Iterate through the fields and reset them
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.CanSet() {
			field.Set(reflect.Zero(field.Type()))
		}
	}

	return nil
}
