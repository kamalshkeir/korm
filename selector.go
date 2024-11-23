package korm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/kamalshkeir/kstrct"
	"github.com/kamalshkeir/lg"
)

type Selector[T any] struct {
	nested  bool
	debug   bool
	ctx     context.Context
	db      *DatabaseEntity
	dest    *[]T
	nocache bool
}

type JsonOption struct {
	As       string
	Dialect  string
	Database string
	Params   []any
}

func JSON_EXTRACT(dataJson string, opt ...JsonOption) string {
	var opts JsonOption
	if len(opt) > 0 {
		opts = opt[0]
	} else {
		return JSON_CAST(dataJson, "")
	}
	if opts.Dialect == "" {
		if opts.Database != "" {
			// db specified
			db, err := GetMemoryDatabase(opts.Database)
			if err != nil {
				db = &databases[0]
			}
			if opts.Dialect == "" {
				opts.Dialect = db.Dialect
			}
		} else {
			opts.Dialect = databases[0].Dialect
		}
	}

	isData := ""
	if strings.HasPrefix(dataJson, "'") {
		isData = "'"
	} else if strings.HasPrefix(dataJson, "{") {
		isData = "{"
	}

	switch opts.Dialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_EXTRACT("
		if isData == "{" {
			st += "'" + dataJson + "'"
		} else {
			st += dataJson
		}
		if len(opts.Params) == 0 {
			st += "),'$'"
			return st
		}
		st += ","

		for i, pp := range opts.Params {
			jsonParam := ""
			if v, ok := pp.(string); ok {
				jsonParam = v
			} else {
				jsonParam = fmt.Sprint(pp)
			}
			if i > 0 {
				st += ","
			}
			if strings.HasPrefix(jsonParam, "'") || strings.HasPrefix(jsonParam, "`") {
				jsonParam = jsonParam[1 : len(jsonParam)-2]
			}
			st += "'$"
			if strings.Contains(jsonParam, ".") {
				sp := strings.Split(jsonParam, ".")
				for _, s := range sp {
					// check if s is a number
					if _, err := strconv.Atoi(s); err == nil {
						st += "[" + s + "]"
					} else {
						st += "." + s
					}
				}
				st += "'"
			} else {
				st += "." + jsonParam + "'"
			}
			if i == len(opts.Params)-1 {
				st += ")"
			}
		}
		if opts.As != "" {
			st += " AS " + opts.As
		}
		return st
	case POSTGRES, "pg":
		if len(opts.Params) == 0 {
			return dataJson
		}
		if isData == "'" || isData == "{" {
			if isData == "{" {
				dataJson = "'" + dataJson + "'::jsonb"
			} else {
				dataJson = dataJson + "::jsonb"
			}
		}

		if len(opts.Params) == 1 {
			ss := ""
			if param, ok := opts.Params[0].(string); ok {
				for _, s := range strings.Split(param, ".") {
					ss += ", '" + s + "'"
				}
				return "JSONB_EXTRACT_PATH_TEXT(" + dataJson + ss + ")"
			} else {
				lg.ErrorC("param must be string", "param", param)
				return dataJson
			}
		}
		paramsString := ""
		// opts.ParamsToExtract "a.2.name", "b.email", "b"
		for i, extractParam := range opts.Params {
			ep := ""
			var ok bool
			if ep, ok = extractParam.(string); !ok {
				lg.ErrorC("param must be string", "param", extractParam)
				return dataJson
			}
			if i > 0 {
				paramsString += ", "
			}
			paramsString += "JSONB_EXTRACT_PATH_TEXT(" + dataJson
			for _, s := range strings.Split(ep, ".") {
				paramsString += ", '" + s + "'"
			}
			paramsString += ")"
		}
		if len(opts.Params) > 1 {
			paramsString = "JSONB_BUILD_ARRAY(" + paramsString + ")"
		}
		if opts.As != "" {
			paramsString += " AS " + opts.As
		}
		return paramsString
	default:
		lg.ErrorC("case not handled", "dialect", opts.Dialect)
		return ""
	}
}

func JSON_REMOVE(dataJson string, opt ...JsonOption) string {
	var opts JsonOption
	if len(opt) > 0 {
		opts = opt[0]
	} else {
		return JSON_CAST(dataJson, "")
	}
	if opts.Dialect == "" {
		if opts.Database != "" {
			// db specified
			db, err := GetMemoryDatabase(opts.Database)
			if err != nil {
				db = &databases[0]
			}
			if opts.Dialect == "" {
				opts.Dialect = db.Dialect
			}
		} else {
			opts.Dialect = databases[0].Dialect
		}
	}

	isData := ""
	if strings.HasPrefix(dataJson, "'") {
		isData = "'"
	} else if strings.HasPrefix(dataJson, "{") {
		isData = "{"
	}

	switch opts.Dialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_REMOVE("
		if isData == "{" {
			st += "'" + dataJson + "'"
		} else {
			st += dataJson
		}
		if len(opts.Params) == 0 {
			st += "),'$'"
			return st
		}
		st += ","

		for i, pp := range opts.Params {
			if i > 0 {
				st += ","
			}
			var jsonParam string
			var ok bool
			if jsonParam, ok = pp.(string); !ok {
				lg.ErrorC("expected string", "param", pp)
				return st
			}

			if i%2 == 0 {
				st += "'$"
				if strings.Contains(jsonParam, ".") {
					sp := strings.Split(jsonParam, ".")
					for _, s := range sp {
						// check if s is a number
						if _, err := strconv.Atoi(s); err == nil {
							st += "[" + s + "]"
						} else {
							st += "." + s
						}
					}
					st += "'"
				} else {
					jsonParam = strings.ReplaceAll(jsonParam, "'", "")
					st += "." + jsonParam + "'"
				}
				if i == len(opts.Params)-1 {
					st += ")"
				}
			} else {
				st += "'$"
				if _, err := strconv.Atoi(jsonParam); err == nil {
					st += "[" + jsonParam + "]"
				} else {
					st += "." + jsonParam + "'"
				}
				if i == len(opts.Params)-1 {
					st += ")"
				}
			}

		}
		if opts.As != "" {
			st += " AS " + opts.As
		}
		return st
	case POSTGRES, "pg":
		// your_column_name #- '{a, 2, name}' #- '{b, email}'
		if isData == "'" || isData == "{" {
			if isData == "{" {
				dataJson = "'" + dataJson + "'::jsonb"
			} else {
				dataJson = dataJson + "::jsonb"
			}
		}
		st := dataJson
		if len(opts.Params) == 0 {
			return st
		}
		for i, pp := range opts.Params {
			var jsonParam string
			var ok bool
			if jsonParam, ok = pp.(string); !ok && i%2 == 0 {
				lg.ErrorC("expected string", "param", jsonParam)
				return st
			}
			if i%2 == 0 && strings.Contains(jsonParam, ".") {
				sp := strings.Split(jsonParam, ".")
				tt := ""
				for i, s := range sp {
					if i > 0 {
						tt += ","
					}
					tt += s
				}
				st += " #- '{" + tt + "}'"
			} else {
				st += " - " + jsonParam
			}
		}
		if opts.As != "" {
			st += " AS " + opts.As
		}
		return st

	default:
		lg.ErrorC("case not handled", "dialect", opts.Dialect)
		return ""
	}
}

func JSON_SET(dataJson string, opt ...JsonOption) string {
	var opts JsonOption
	if len(opt) > 0 {
		opts = opt[0]
	} else {
		return JSON_CAST(dataJson, "data")
	}
	if len(opts.Params)%2 != 0 {
		lg.ErrorC("expected even number of params", "params", opts.Params)
		return dataJson
	}
	if opts.Dialect == "" {
		if opts.Database != "" {
			// db specified
			db, err := GetMemoryDatabase(opts.Database)
			if err != nil {
				db = &databases[0]
			}
			if opts.Dialect == "" {
				opts.Dialect = db.Dialect
			}
		} else {
			opts.Dialect = databases[0].Dialect
		}
	}

	isData := ""
	if strings.HasPrefix(dataJson, "'") {
		isData = "'"
	} else if strings.HasPrefix(dataJson, "{") {
		isData = "{"
	}

	switch opts.Dialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_SET("
		if isData == "{" {
			st += "'" + dataJson + "'"
		} else {
			st += dataJson
		}
		if len(opts.Params) == 0 {
			st += "),'$'"
			return st
		}
		st += ","

		for i, pp := range opts.Params {
			if i > 0 {
				st += ","
			}
			var jsonParam string
			var ok bool
			if i%2 == 0 {
				if jsonParam, ok = pp.(string); !ok {
					lg.ErrorC("expected string", "param", pp)
					return st
				}
				jsonParam = strings.ReplaceAll(jsonParam, "'", "")
				st += "'$"
				if strings.Contains(jsonParam, ".") {
					sp := strings.Split(jsonParam, ".")
					for _, s := range sp {
						// check if s is a number
						if _, err := strconv.Atoi(s); err == nil {
							st += "[" + s + "]"
						} else {
							st += "." + s
						}
					}
					st += "'"
				} else {
					st += "." + jsonParam + "'"
				}
			} else {
				if jp, ok := pp.(string); ok {
					if !strings.Contains(jp, "'") {
						jp = "'" + jp + "'"
					}
					st += jp
				} else {
					st += fmt.Sprint(pp)
				}
			}

			if i == len(opts.Params)-1 {
				st += ")"
			}
		}
		if opts.As != "" {
			st += " AS " + opts.As
		}
		return st
	case POSTGRES, "pg":
		if isData == "'" || isData == "{" {
			if isData == "{" {
				dataJson = "'" + dataJson + "'::jsonb"
			} else {
				dataJson = dataJson + "::jsonb"
			}
		}
		newws := make([]string, 0, len(opts.Params)/2)
		st := "JSONB_SET(" + dataJson
		if len(opts.Params) == 0 {
			return st + ")"
		} else if len(opts.Params) == 2 {
			for i, pp := range opts.Params {
				var jsonParam string
				var ok bool
				if i == 0 {
					if jsonParam, ok = pp.(string); !ok {
						lg.ErrorC("expected string", "param", pp)
						return dataJson
					}
				}
				st += ", "
				if i%2 == 0 && strings.Contains(jsonParam, ".") {
					tt := strings.ReplaceAll(jsonParam, ".", ",")
					tt = strings.ReplaceAll(tt, "'", "")
					st += " '{" + tt + "}'"
				} else {
					if v, ok := pp.(string); ok {
						if !strings.Contains(v, "'") {
							st += " '" + v + "'"
						} else {
							st += v
						}
					} else {
						st += fmt.Sprint(pp)
					}
				}
			}
			st += ", 'true'"
		} else {
			new := "JSONB_SET(" + dataJson
			for i, pp := range opts.Params {
				var jsonParam string
				var ok bool
				if i%2 == 0 {
					new = "JSONB_SET(%s"
					if jsonParam, ok = pp.(string); !ok {
						lg.ErrorC("expected string", "param", pp)
						return dataJson
					} else {
						pp = strings.ReplaceAll(jsonParam, "'", "")
					}
				}
				new += ", "
				if i%2 == 0 {
					tt := strings.ReplaceAll(jsonParam, ".", ",")
					new += " '{" + tt + "}'"
				} else {
					if v, ok := pp.(string); ok && i%2 == 1 {
						if !strings.Contains(v, "'") {
							new += " '" + v + "', 'true'"
						} else {
							new += " " + v + ", 'true'"
						}
					} else {
						new += fmt.Sprint(pp) + ", 'true'"
					}
					new += ")"
					newws = append(newws, new)
				}
			}
			res := newws[0]
			for i, v := range newws {
				if i == 0 {
					continue
				}
				res = fmt.Sprintf(res, v)
				if i == len(newws)-1 {
					res = fmt.Sprintf(res, dataJson)
				}
			}
			if opts.As != "" {
				res += " AS " + opts.As
			}
			return res
		}
		st += ")"
		if opts.As != "" {
			st += " AS " + opts.As
		}
		return st
	default:
		lg.ErrorC("case not handled", "dialect", opts.Dialect)
		return ""
	}
}

func JSON_ARRAY(values []any, as string, dialect ...string) string {
	dbDialect := databases[0].Dialect
	if len(dialect) > 0 {
		dbDialect = dialect[0]
	}
	valuesString := make([]string, 0, len(values))
	for _, v := range values {
		switch vType := v.(type) {
		case string:
			if strings.Contains(vType, "'") {
				valuesString = append(valuesString, vType)
			} else {
				valuesString = append(valuesString, "'"+vType+"'")
			}
		case time.Time:
			valuesString = append(valuesString, fmt.Sprint(vType.Unix()))
		case *time.Time:
			valuesString = append(valuesString, fmt.Sprint(vType.Unix()))
		default:
			valuesString = append(valuesString, fmt.Sprint(v))
		}
	}

	switch dbDialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_ARRAY(" + strings.Join(valuesString, ", ") + ")"
		if as != "" {
			st += " AS " + as
		}
		return st
	case POSTGRES, "pg":
		st := "JSONB_BUILD_ARRAY(" + strings.Join(valuesString, ", ") + ")"
		if as != "" {
			st += " AS " + as
		}
		return st
	default:
		lg.ErrorC("case not handled", "dialect", dbDialect)
		return ""
	}
}

func JSON_OBJECT(values []any, as string, dialect ...string) string {
	dbDialect := databases[0].Dialect
	if len(dialect) > 0 {
		dbDialect = dialect[0]
	}
	valuesString := make([]string, 0, len(values))
	for i, v := range values {
		var vv string
		if i%2 == 0 {
			var ok bool
			if vv, ok = v.(string); ok {
				if !strings.HasPrefix(vv, "'") {
					vv = "'" + vv + "'"
				}
			} else {
				lg.ErrorC("expected string", "value", v)
				return ""
			}
		} else {
			switch val := v.(type) {
			case string:
				if !strings.HasPrefix(val, "'") {
					vv = "'" + val + "'"
				} else {
					vv = val
				}
			case time.Time:
				vv = fmt.Sprint(val.Unix())
			case *time.Time:
				vv = fmt.Sprint(val.Unix())
			default:
				vv = fmt.Sprint(v)
			}
		}

		valuesString = append(valuesString, vv)
	}

	switch dbDialect {
	case SQLITE, MYSQL, "sqlite":
		st := "JSON_OBJECT(" + strings.Join(valuesString, ", ") + ")"
		if as != "" {
			st += " AS " + as
		}
		return st
	case POSTGRES, "pg":
		st := "JSONB_BUILD_OBJECT(" + strings.Join(valuesString, ", ") + ")"
		if as != "" {
			st += " AS " + as
		}
		return st
	default:
		lg.ErrorC("case not handled", "dialect", dbDialect)
		return ""
	}
}

func JSON_CAST(value string, as string, dialect ...string) string {
	value = strings.ReplaceAll(value, "'", "\"")
	dbDialect := databases[0].Dialect
	if len(dialect) > 0 {
		dbDialect = dialect[0]
	}
	if !strings.HasPrefix(value, "'") {
		value = "'" + value + "'"
	}
	var st string
	switch dbDialect {
	case SQLITE, "sqlite":
		st = "JSON(" + value + ")"
	case POSTGRES, "pg":
		st = value + "::jsonb"
	case MYSQL:
		st = "JSON_EXTRACT(" + value + ", '$')"
	}
	if as != "" {
		st += " AS " + as
	}
	return st
}

func To[T any](dest *[]T, nestedSlice ...bool) *Selector[T] {
	s := &Selector[T]{
		dest: dest,
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
	} else {
		lg.ErrorC("db not found", "dbname", dbName)
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

func (sl *Selector[T]) Query(statement string, args ...any) error {
	var stt string
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

	adaptPlaceholdersToDialect(&statement, sl.db.Dialect)
	adaptTimeToUnixArgs(&args)
	var rows *sql.Rows
	var err error
	if sl.debug {
		lg.Info("DEBUG SELECTOR", "statement", statement, "args", args)
	}
	if sl.ctx != nil {
		rows, err = sl.db.Conn.QueryContext(sl.ctx, statement, args...)
	} else {
		rows, err = sl.db.Conn.Query(statement, args...)
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
		if sl.db.Dialect == MYSQL {
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
					if lg.CheckError(err) {
						return err
					}
					*sl.dest = append(*sl.dest, newElem)
				} else {
					err := kstrct.FillFromKV(temp, kv)
					if lg.CheckError(err) {
						return err
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
					err := kstrct.FillFromKV(*temp, kv, true)
					if lg.CheckError(err) {
						return err
					}
				} else {
					*sl.dest = append(*sl.dest, *new(T))
					temp = &(*sl.dest)[0]
					err := kstrct.FillFromKV(temp, kv, true)
					if lg.CheckError(err) {
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
								if lg.CheckError(err) {
									return err
								}
							} else {
								*sl.dest = append(*sl.dest, *new(T))
								temp = &(*sl.dest)[index]
								err := kstrct.FillFromKV(temp, kv, true)
								if lg.CheckError(err) {
									return err
								}
							}
						} else {
							lastData = append(lastData, kvv)
							if !isPtr {
								err := kstrct.FillFromKV(&(*sl.dest)[index], kv, true)
								if lg.CheckError(err) {
									return err
								}
							} else {
								err := kstrct.FillFromKV((*sl.dest)[index], kv, true)
								if lg.CheckError(err) {
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
					if lg.CheckError(err) {
						return err
					}
					continue loop
				} else {
					err := kstrct.FillFromKV((*sl.dest)[0], kv, true)
					if lg.CheckError(err) {
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
						if lg.CheckError(err) {
							return err
						}
					} else {
						err := kstrct.SetReflectFieldValue(chanType, vKv.Value)
						if lg.CheckError(err) {
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
	var stt string
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
		query, newargs, err = AdaptNamedParams(sl.db.Dialect, statement, args)
		if err != nil {
			return err
		}
	}
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
		if sl.db.Dialect == MYSQL {
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
					if lg.CheckError(err) {
						return err
					}
					*sl.dest = append(*sl.dest, newElem)
				} else {
					err := kstrct.FillFromKV(temp, kv)
					if lg.CheckError(err) {
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
					if lg.CheckError(err) {
						return err
					}
				} else {
					*sl.dest = append(*sl.dest, *new(T))
					temp = &(*sl.dest)[0]
					err := kstrct.FillFromKV(temp, kv, true)
					if lg.CheckError(err) {
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
								if lg.CheckError(err) {
									return err
								}
							} else {
								*sl.dest = append(*sl.dest, *new(T))
								temp = &(*sl.dest)[index]
								err := kstrct.FillFromKV(temp, kv, true)
								if lg.CheckError(err) {
									return err
								}
							}
						} else {
							lastData = append(lastData, kvv)
							if !isPtr {
								err := kstrct.FillFromKV(&(*sl.dest)[index], kv, true)
								if lg.CheckError(err) {
									return err
								}
							} else {
								err := kstrct.FillFromKV((*sl.dest)[index], kv, true)
								if lg.CheckError(err) {
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
					if lg.CheckError(err) {
						return err
					}
					continue loop
				} else {
					err := kstrct.FillFromKV((*sl.dest)[0], kv, true)
					if lg.CheckError(err) {
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
						if lg.CheckError(err) {
							return err
						}
					} else {
						err := kstrct.SetReflectFieldValue(chanType, vKv.Value)
						if lg.CheckError(err) {
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
