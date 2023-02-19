package korm

import (
	"strconv"
	"strings"
	"time"
)

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

func adaptTrueFalseArgs(args *[]any) {
	for i := range *args {
		if (*args)[i] == true {
			(*args)[i] = 1
		} else if (*args)[i] == false {
			(*args)[i] = 0
		} else if v, ok := (*args)[i].(time.Time); ok {
			(*args)[i] = v.Unix()
		} else if v, ok := (*args)[i].(*time.Time); ok {
			(*args)[i] = v.Unix()
		}
	}
}

func adaptWhereQuery(query *string, tableName ...string) {
	tbName := ""
	if len(tableName) > 0 {
		tbName = tableName[0]
	}
	*query = strings.ToLower(*query)
	q := []rune(*query)
	hasComparaisonSign := false
	hasQuestionMark := false
	for i := range q {
		switch q[i] {
		case '?':
			hasQuestionMark = true
		case '=', '>', '<', '!':
			hasComparaisonSign = true
		case 'l':
			if i+3 <= len(q)-1 {
				if q[i+1] == 'i' && q[i+2] == 'k' && q[i+3] == 'e' {
					hasComparaisonSign = true
				}
			}
		}
	}

	if !hasQuestionMark {
		var b strings.Builder
		fieldStart := -1
		for i, c := range q {
			if c == ',' || c == '|' {
				if fieldStart >= 0 {
					if tbName != "" && !strings.Contains(string(q[fieldStart:i]), "(") {
						b.WriteString(tbName)
						b.WriteString(".")
					}
					b.WriteString(string(q[fieldStart:i]))
					if !hasComparaisonSign {
						b.WriteString(" = ?")
					}
					if i < len(q)-1 {
						if c == '|' {
							b.WriteString(" OR ")
						} else {
							b.WriteString(" AND ")
						}
					}
					fieldStart = -1
				}
			} else if fieldStart < 0 {
				fieldStart = i
			}
		}
		if fieldStart >= 0 {
			if tbName != "" && !strings.Contains(string(q[fieldStart:]), "(") {
				b.WriteString(tbName)
				b.WriteString(".")
			}
			b.WriteString(string(q[fieldStart:]))
			if !hasComparaisonSign {
				b.WriteString(" = ?")
			}
		}
		*query = b.String()
	} else {
		spAnd := strings.Split(*query, "and")
		tbToAdd := false
		for i := range spAnd {
			spOr := strings.Split(spAnd[i], "or")
			for j := range spOr {
				if tbToAdd || (tbName != "" && !strings.HasPrefix(spOr[j], tbName) && !strings.Contains((*query)[i:], "(")) {
					if !tbToAdd {
						tbToAdd = true
					}
					spOr[j] = tbName + "." + strings.TrimSpace(spOr[j])
				}
				spAnd[i] = strings.Join(spOr, " OR ")
			}
		}
		*query = strings.Join(spAnd, " AND ")
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
