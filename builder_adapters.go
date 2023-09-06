package korm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kamalshkeir/klog"
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
		buf.WriteString(statement[lastIndex:start])
		if len(unsafe) > 0 && unsafe[0] {
			if v, ok := value.(string); ok {
				_, err := buf.WriteString(v)
				klog.CheckError(err)
			} else {
				_, err := buf.WriteString(fmt.Sprint(value))
				klog.CheckError(err)
			}
		} else {
			buf.WriteString("?")
		}
		anys = append(anys, value)
		lastIndex = end
	}
	buf.WriteString(statement[lastIndex:])
	res := buf.String()
	if len(unsafe) == 0 {
		adaptPlaceholdersToDialect(&res, dialect)
	}
	return res, anys, nil
}

func adaptPlaceholdersToDialect(query *string, dialect string) {
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
	str = strings.ToLower(str)
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
}
