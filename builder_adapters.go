package korm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kamalshkeir/lg"
)

func UnsafeNamedQuery(query string, args map[string]any) (string, error) {
	q, _, err := AdaptNamedParams("", query, args, true)
	if err != nil {
		return "", err
	}
	return q, nil
}

func AdaptNamedParams(dialect, statement string, variables map[string]any, unsafe ...bool) (string, []any, error) {
	if !strings.Contains(statement, ":") {
		return statement, nil, nil
	}
	var paramCount int
	for i := 0; i < len(statement); i++ {
		if statement[i] == ':' {
			paramCount++
			for i < len(statement) && statement[i] != ' ' && statement[i] != ',' && statement[i] != ')' {
				i++
			}
		}
	}
	anys := make([]any, 0, paramCount)
	buf := strings.Builder{}
	lastIndex := 0
	for {
		index := strings.Index(statement[lastIndex:], ":")
		if index == -1 {
			break
		}
		start := lastIndex + index
		end := start
		for end < len(statement) && statement[end] != ' ' && statement[end] != ',' && statement[end] != ')' {
			end++
		}
		key := statement[start+1 : end]
		value, ok := variables[key]
		if !ok {
			return "", nil, fmt.Errorf("missing variable value for '%s'", key)
		}
		switch vt := value.(type) {
		case time.Time:
			value = vt.Unix()
		case *time.Time:
			value = vt.Unix()
		}

		// Handle IN clause values
		beforeParam := strings.TrimSpace(strings.ToUpper(statement[max(0, start-5):start]))
		isInClause := strings.HasSuffix(beforeParam, "IN") || strings.HasSuffix(beforeParam, "IN (")

		buf.WriteString(statement[lastIndex:start])
		if len(unsafe) > 0 && unsafe[0] {
			if v, ok := value.(string); ok {
				_, err := buf.WriteString(v)
				lg.CheckError(err)
			} else {
				_, err := buf.WriteString(fmt.Sprint(value))
				lg.CheckError(err)
			}
		} else if isInClause {
			// Handle different slice types for IN clause
			switch v := value.(type) {
			case []int:
				buf.WriteString(strings.Repeat("?,", len(v)-1) + "?")
				for _, val := range v {
					anys = append(anys, val)
				}
			case []uint:
				buf.WriteString(strings.Repeat("?,", len(v)-1) + "?")
				for _, val := range v {
					anys = append(anys, val)
				}
			case []string:
				buf.WriteString(strings.Repeat("?,", len(v)-1) + "?")
				for _, val := range v {
					anys = append(anys, val)
				}
			case []any:
				buf.WriteString(strings.Repeat("?,", len(v)-1) + "?")
				anys = append(anys, v...)
			default:
				buf.WriteString("?")
				anys = append(anys, value)
			}
		} else {
			buf.WriteString("?")
			switch vt := value.(type) {
			case time.Time:
				value = vt.Unix()
			case *time.Time:
				value = vt.Unix()
			case string:
				value = "'" + vt + "'"
			}
			anys = append(anys, value)
		}
		lastIndex = end
	}
	buf.WriteString(statement[lastIndex:])
	res := buf.String()
	if len(unsafe) == 0 || !unsafe[0] {
		AdaptPlaceholdersToDialect(&res, dialect)
	}
	return res, anys, nil
}

// Helper function since Go doesn't have a built-in max for ints
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func AdaptPlaceholdersToDialect(query *string, dialect string) {
	if strings.Contains(*query, "?") && (dialect != MYSQL) {
		split := strings.Split(*query, "?")
		counter := 0
		for i := range split {
			if i < len(split)-1 {
				counter++
				split[i] = split[i] + "$" + strconv.Itoa(counter)
			}
		}
		*query = strings.Join(split, "")
	}
}

func adaptTimeToUnixArgs(args *[]any) {
	for i := range *args {
		switch v := (*args)[i].(type) {
		case time.Time:
			(*args)[i] = v.Unix()
		case *time.Time:
			(*args)[i] = v.Unix()
		}
	}
}

func adaptSetQuery(query *string) {
	sp := strings.Split(*query, ",")
	q := []rune(*query)
	hasQuestionMark := false
	hasEqual := false
	for i := range q {
		if q[i] == '?' {
			hasQuestionMark = true
		} else if q[i] == '=' {
			hasEqual = true
		}
	}
	for i := range sp {
		if !hasQuestionMark && !hasEqual {
			sp[i] = sp[i] + "= ?"
		}
	}
	*query = strings.Join(sp, ",")
}

func adaptConcatAndLen(str string, dialect Dialect) string {
	if strings.Contains(str, "len(") || strings.Contains(str, "concat") {
		if dialect == SQLITE {
			strt := strings.Replace(str, "len(", "length(", -1)
			if str != strt {
				str = strt
			} else {
				str = strings.Replace(str, "len (", "length (", -1)
			}
		} else {
			strt := strings.Replace(str, "len(", "char_length(", -1)
			if str != strt {
				str = strt
			} else {
				str = strings.Replace(str, "len (", "char_length (", -1)
			}
		}

		start := strings.Index(str, "concat")
		if start == -1 || (dialect != SQLITE && dialect != "") {
			return str
		}
		// only for sqlite3
		parenthesis1 := strings.Index(str[start:], "(")
		parenthesis2 := strings.Index(str[start:], ")")
		inside := str[start+parenthesis1+1 : start+parenthesis2]
		sp := strings.Split(inside, ",")
		var result string
		for i, val := range sp {
			val = strings.TrimSpace(val)
			if i == 0 {
				result = val
			} else {
				result += " || " + val
			}
		}
		res := str[:start] + result + str[start+parenthesis2+1:]
		return res
	} else {
		return str
	}

}

// In expands slice arguments for IN clauses before dialect adaptation
func In(query string, args ...any) (string, []any) {
	if !strings.Contains(query, "?") {
		return query, args
	}

	var expandedArgs []any
	split := strings.Split(query, "?")
	var result strings.Builder
	argIndex := 0

	for i := range split {
		result.WriteString(split[i])
		if i < len(split)-1 && argIndex < len(args) {
			// Check if this placeholder is part of an IN clause
			beforePlaceholder := strings.TrimSpace(strings.ToUpper(split[i]))
			if strings.HasSuffix(beforePlaceholder, "IN") || strings.HasSuffix(beforePlaceholder, "IN (") {
				// Handle slice for IN clause
				switch v := args[argIndex].(type) {
				case []int:
					// Convert []int to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []string:
					// Convert []string to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []int64:
					// Convert []int to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []uint:
					// Convert []uint to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []uint8:
					// Convert []uint to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []int32:
					// Convert []int to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []int16:
					// Convert []int to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []int8:
					// Convert []int to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []uint16:
					// Convert []uint to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []uint32:
					// Convert []uint to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []float32:
					// Convert []uint to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []float64:
					// Convert []uint to []any
					anySlice := make([]any, len(v))
					for i, val := range v {
						anySlice[i] = val
					}
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, anySlice...)
				case []any:
					result.WriteString(strings.Repeat("?,", len(v)-1) + "?")
					expandedArgs = append(expandedArgs, v...)
				default:
					// Not a slice, treat as normal arg
					result.WriteString("?")
					expandedArgs = append(expandedArgs, args[argIndex])
				}
			} else {
				// Normal argument
				result.WriteString("?")
				expandedArgs = append(expandedArgs, args[argIndex])
			}
			argIndex++
		}
	}

	return result.String(), expandedArgs
}
