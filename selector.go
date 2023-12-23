package korm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kstrct"
)

type Selector[T any] struct {
	dbName  string
	nested  bool
	debug   bool
	ctx     context.Context
	dest    *[]T
	nocache bool
}

func To[T any](dest *[]T, nestedSlice ...bool) *Selector[T] {
	if len(nestedSlice) > 0 && nestedSlice[0] {
		return &Selector[T]{
			nested: true,
			dest:   dest,
		}
	} else {
		return &Selector[T]{
			nested: false,
			dest:   dest,
		}
	}
}

func (sl *Selector[T]) Database(dbName string) *Selector[T] {
	sl.dbName = dbName
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

func (sl *Selector[T]) Query(statement string, args ...any) error {
	var db *DatabaseEntity
	var stt string
	if sl.dbName != "" {
		var err error
		db, err = GetMemoryDatabase(sl.dbName)
		if err != nil {
			return err
		}
	} else {
		db = &databases[0]
	}
	if useCache && !sl.nocache {
		stt = statement + fmt.Sprint(args...)
		if v, ok := cacheQ.Get(stt); ok {
			if len(*sl.dest) == 0 {
				*sl.dest = v.([]T)
				return nil
			}
		}
	}

	typ := fmt.Sprintf("%T", *new(T))
	ref := reflect.ValueOf(*new(T))

	adaptPlaceholdersToDialect(&statement, db.Dialect)
	adaptTimeToUnixArgs(&args)
	var rows *sql.Rows
	var err error
	if sl.debug {
		klog.Printfs("yl%s , args: %v", statement, args)
	}
	if sl.ctx != nil {
		rows, err = db.Conn.QueryContext(sl.ctx, statement, args...)
	} else {
		rows, err = db.Conn.Query(statement, args...)
	}
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
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
			return err
		}
		if db.Dialect == MYSQL {
			for i, kvv := range kv {
				if v, ok := kvv.Value.([]byte); ok {
					kv[i] = kstrct.KV{Key: kvv.Key, Value: string(v)}
				}
			}
		}
		switch {
		case isStrct && !isChan:
			if !isNested {
				if isPtr {
					t := ref.Type().Elem()
					newElem := reflect.New(t).Interface().(T)
					err := kstrct.FillFromKV(newElem, kv)
					if klog.CheckError(err) {
						return err
					}
					*sl.dest = append(*sl.dest, newElem)
				} else {
					err := kstrct.FillFromKV(temp, kv)
					if klog.CheckError(err) {
						return err
					}
					*sl.dest = append(*sl.dest, *temp)
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
					err := kstrct.FillFromKV(*temp, kv, true)
					if klog.CheckError(err) {
						return err
					}
				} else {
					*sl.dest = append(*sl.dest, *new(T))
					temp = &(*sl.dest)[0]
					err := kstrct.FillFromKV(temp, kv, true)
					if klog.CheckError(err) {
						return err
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
								err := kstrct.FillFromKV(*temp, kv, true)
								if klog.CheckError(err) {
									return err
								}
							} else {
								*sl.dest = append(*sl.dest, *new(T))
								temp = &(*sl.dest)[index]
								err := kstrct.FillFromKV(temp, kv, true)
								if klog.CheckError(err) {
									return err
								}
							}
						} else {
							lastData = append(lastData, kvv)
							if !isPtr {
								err := kstrct.FillFromKV(&(*sl.dest)[index], kv, true)
								if klog.CheckError(err) {
									return err
								}
							} else {
								err := kstrct.FillFromKV((*sl.dest)[index], kv, true)
								if klog.CheckError(err) {
									return err
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
				return fmt.Errorf("map is not nested struct")
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
				return fmt.Errorf("arithmetic types cannot be nested")
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
								return err
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
								return err
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
					return fmt.Errorf("channel of pointers not allowed in case of structs")
				}
				if !isNested {
					err := kstrct.FillFromKV((*sl.dest)[0], kv)
					if klog.CheckError(err) {
						return err
					}
					continue loop
				} else {
					err := kstrct.FillFromKV((*sl.dest)[0], kv, true)
					if klog.CheckError(err) {
						return err
					}
					continue loop
				}

			case isMap:
				if isNested {
					return fmt.Errorf("map cannot be nested")
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
					return fmt.Errorf("type cannot be nested")
				}
				chanType := reflect.New(ref.Type().Elem()).Elem()
				for _, vKv := range kv {
					if chanType.Kind() == reflect.Struct || (chanType.Kind() == reflect.Ptr && chanType.Elem().Kind() == reflect.Struct) {
						m := make(map[string]any, len(kv))
						for _, vkv := range kv {
							m[vkv.Key] = vkv.Value
						}
						err := kstrct.SetReflectFieldValue(chanType, m)
						if klog.CheckError(err) {
							return err
						}
					} else {
						err := kstrct.SetReflectFieldValue(chanType, vKv.Value)
						if klog.CheckError(err) {
							return err
						}
					}
					reflect.ValueOf((*sl.dest)[0]).Send(chanType)
				}
			default:
				return fmt.Errorf("channel case not handled")
			}
		default:
			return fmt.Errorf("default triggered, case not handled")
		}
	}
	if useCache && !sl.nocache && !isChan && len(*sl.dest) > 0 {
		cacheQ.Set(stt, *sl.dest)
	}
	return nil
}

func (sl *Selector[T]) Named(statement string, args map[string]any, unsafe ...bool) error {
	var db *DatabaseEntity
	var stt string
	if sl.dbName != "" {
		var err error
		db, err = GetMemoryDatabase(sl.dbName)
		if err != nil {
			return err
		}
	} else {
		db = &databases[0]
	}
	if useCache && !sl.nocache {
		stt = statement + fmt.Sprint(args)
		if v, ok := cacheQ.Get(stt); ok {
			if len(*sl.dest) == 0 {
				*sl.dest = v.([]T)
				return nil
			}
		}
	}

	typ := fmt.Sprintf("%T", *new(T))
	ref := reflect.ValueOf(sl.dest)

	for i := range args {
		switch v := args[i].(type) {
		case time.Time:
			args[i] = v.Unix()
		case *time.Time:
			args[i] = v.Unix()
		}
	}
	var query string
	var newargs []any
	if len(unsafe) > 0 && unsafe[0] {
		var err error
		query, err = UnsafeNamedQuery(statement, args)
		if err != nil {
			return err
		}
	} else {
		var err error
		query, newargs, err = AdaptNamedParams(db.Dialect, statement, args)
		if err != nil {
			return err
		}
	}
	var rows *sql.Rows
	var err error
	if sl.debug {
		klog.Printfs("yl%s , args: %v", query, newargs)
	}
	if sl.ctx != nil {
		rows, err = db.Conn.QueryContext(sl.ctx, query, newargs...)
	} else {
		rows, err = db.Conn.Query(query, newargs...)
	}
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
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
			return err
		}
		if db.Dialect == MYSQL {
			for i, kvv := range kv {
				if v, ok := kvv.Value.([]byte); ok {
					kv[i] = kstrct.KV{Key: kvv.Key, Value: string(v)}
				}
			}
		}
		switch {
		case isStrct && !isChan:
			if !isNested {
				if isPtr {
					t := ref.Type().Elem()
					newElem := reflect.New(t).Interface().(T)
					err := kstrct.FillFromKV(newElem, kv)
					if klog.CheckError(err) {
						return err
					}
					*sl.dest = append(*sl.dest, newElem)
				} else {
					err := kstrct.FillFromKV(temp, kv)
					if klog.CheckError(err) {
						return err
					}
					*sl.dest = append(*sl.dest, *temp)
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
					err := kstrct.FillFromKV(*temp, kv, true)
					if klog.CheckError(err) {
						return err
					}
				} else {
					*sl.dest = append(*sl.dest, *new(T))
					temp = &(*sl.dest)[0]
					err := kstrct.FillFromKV(temp, kv, true)
					if klog.CheckError(err) {
						return err
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
								err := kstrct.FillFromKV(*temp, kv, true)
								if klog.CheckError(err) {
									return err
								}
							} else {
								*sl.dest = append(*sl.dest, *new(T))
								temp = &(*sl.dest)[index]
								err := kstrct.FillFromKV(temp, kv, true)
								if klog.CheckError(err) {
									return err
								}
							}
						} else {
							lastData = append(lastData, kvv)
							if !isPtr {
								err := kstrct.FillFromKV(&(*sl.dest)[index], kv, true)
								if klog.CheckError(err) {
									return err
								}
							} else {
								err := kstrct.FillFromKV((*sl.dest)[index], kv, true)
								if klog.CheckError(err) {
									return err
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
				return fmt.Errorf("map is not nested struct")
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
				return fmt.Errorf("this type cannot be nested")
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
								return err
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
								return err
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
					return fmt.Errorf("channel of pointers not allowed in case of structs")
				}
				if !isNested {
					err := kstrct.FillFromKV((*sl.dest)[0], kv)
					if klog.CheckError(err) {
						return err
					}
					continue loop
				} else {
					err := kstrct.FillFromKV((*sl.dest)[0], kv, true)
					if klog.CheckError(err) {
						return err
					}
					continue loop
				}
			case isMap:
				if isNested {
					return fmt.Errorf("map cannot be nested")
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
					return fmt.Errorf("type cannot be nested")
				}
				chanType := reflect.New(ref.Type().Elem()).Elem()
				for _, vKv := range kv {
					if chanType.Kind() == reflect.Struct || (chanType.Kind() == reflect.Ptr && chanType.Elem().Kind() == reflect.Struct) {
						m := make(map[string]any, len(kv))
						for _, vkv := range kv {
							m[vkv.Key] = vkv.Value
						}
						err := kstrct.SetReflectFieldValue(chanType, m)
						if klog.CheckError(err) {
							return err
						}
					} else {
						err := kstrct.SetReflectFieldValue(chanType, vKv.Value)
						if klog.CheckError(err) {
							return err
						}
					}
					reflect.ValueOf((*sl.dest)[0]).Send(chanType)
				}
			default:
				return fmt.Errorf("channel case not handled")
			}
		default:
			return fmt.Errorf("default triggered, case not handled")
		}
	}
	if useCache && !sl.nocache && !isChan && len(*sl.dest) > 0 {
		cacheQ.Set(stt, *sl.dest)
	}
	return nil
}
