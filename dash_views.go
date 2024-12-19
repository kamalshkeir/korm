package korm

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kamalshkeir/aes"
	"github.com/kamalshkeir/argon"
	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
)

var termsessions = kmap.New[string, string]()

var LogsView = func(c *ksmux.Context) {
	d := map[string]any{
		"admin_path":       adminPathNameGroup,
		"static_url":       staticUrl,
		"secure":           ksmux.IsTLS,
		"trace_enabled":    defaultTracer.enabled,
		"terminal_enabled": terminalUIEnabled,
	}
	parsed := make([]LogEntry, 0)
	if v := lg.GetLogs(); v != nil {
		for _, vv := range reverseSlice(v.Slice) {
			parsed = append(parsed, parseLogString(vv))
		}
	}
	d["parsed"] = parsed
	c.Html("admin/admin_logs.html", d)
}

var DashView = func(c *ksmux.Context) {
	ddd := map[string]any{
		"admin_path":         adminPathNameGroup,
		"static_url":         staticUrl,
		"withRequestCounter": withRequestCounter,
		"trace_enabled":      defaultTracer.enabled,
		"terminal_enabled":   terminalUIEnabled,
		"stats":              GetStats(),
	}
	if withRequestCounter {
		ddd["requests"] = GetTotalRequests()
	}

	c.Html("admin/admin_index.html", ddd)
}

var TablesView = func(c *ksmux.Context) {
	allTables := GetAllTables(defaultDB)
	q := []string{}
	for _, t := range allTables {
		q = append(q, "SELECT '"+t+"' AS table_name,COUNT(*) AS count FROM "+t)
	}
	query := strings.Join(q, ` UNION ALL `)

	var results []struct {
		TableName string `db:"table_name"`
		Count     int    `db:"count"`
	}
	if err := To(&results).Query(query); lg.CheckError(err) {
		c.Error("something wrong happened")
		return
	}

	c.Html("admin/admin_tables.html", map[string]any{
		"admin_path":       adminPathNameGroup,
		"static_url":       staticUrl,
		"tables":           allTables,
		"results":          results,
		"trace_enabled":    defaultTracer.enabled,
		"terminal_enabled": terminalUIEnabled,
	})
}

var LoginView = func(c *ksmux.Context) {
	c.Html("admin/admin_login.html", map[string]any{
		"admin_path": adminPathNameGroup,
		"static_url": staticUrl,
	})
}

var LoginPOSTView = func(c *ksmux.Context) {
	requestData := c.BodyJson()
	email := requestData["email"]
	passRequest := requestData["password"]

	data, err := Table("users").Database(defaultDB).Where("email = ?", email).One()
	if err != nil {
		c.Status(http.StatusUnauthorized).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	if data["email"] == "" || data["email"] == nil {
		c.Status(http.StatusNotFound).Json(map[string]any{
			"error": "User doesn not Exist",
		})
		return
	}
	if data["is_admin"] == int64(0) || data["is_admin"] == 0 || data["is_admin"] == false {
		c.Status(http.StatusForbidden).Json(map[string]any{
			"error": "Not Allowed to access this page",
		})
		return
	}

	if passDB, ok := data["password"].(string); ok {
		if pp, ok := passRequest.(string); ok {
			if !argon.Match(passDB, pp) {
				c.Status(http.StatusForbidden).Json(map[string]any{
					"error": "Wrong Password",
				})
				return
			} else {
				if uuid, ok := data["uuid"].(string); ok {
					uuid, err = aes.Encrypt(uuid)
					lg.CheckError(err)
					c.SetCookie("session", uuid)
					c.Json(map[string]any{
						"success": "U Are Logged In",
					})
					return
				}
			}
		}
	}
}

var LogoutView = func(c *ksmux.Context) {
	c.DeleteCookie("session")
	c.Status(http.StatusTemporaryRedirect).Redirect("/")
}

var TableGetAll = func(c *ksmux.Context) {
	model := c.Param("model")
	if model == "" {
		c.Json(map[string]any{
			"error": "Error: No model given in params",
		})
		return
	}
	dbMem, _ := GetMemoryDatabase(defaultDB)
	if dbMem == nil {
		lg.ErrorC("unable to find db in mem", "db", defaultDB)
		dbMem = &databases[0]
	}
	idString := "id"
	var t *TableEntity
	for i, tt := range dbMem.Tables {
		if tt.Name == model {
			idString = tt.Pk
			t = &dbMem.Tables[i]
		}
	}

	var body struct {
		Page int `json:"page"`
	}
	if err := c.BodyStruct(&body); lg.CheckError(err) {
		c.Error("something wrong happened")
		return
	}
	if body.Page == 0 {
		body.Page = 1
	}
	rows, err := Table(model).Database(defaultDB).OrderBy("-" + idString).Limit(paginationPer).Page(body.Page).All()
	if err != nil {
		if err != ErrNoData {
			c.Status(404).Error("Unable to find this model")
			return
		}
		rows = []map[string]any{}
	}

	// Get total count for pagination
	var total int64
	var totalRows []int64
	err = To(&totalRows).Query("SELECT COUNT(*) FROM " + model)
	if err == nil {
		total = totalRows[0]
	}

	dbCols, cols := GetAllColumnsTypes(model)
	mmfkeysModels := map[string][]map[string]any{}
	mmfkeys := map[string][]any{}
	if t != nil {
		for _, fkey := range t.Fkeys {
			spFrom := strings.Split(fkey.FromTableField, ".")
			if len(spFrom) == 2 {
				spTo := strings.Split(fkey.ToTableField, ".")
				if len(spTo) == 2 {
					q := "select * from " + spTo[0] + " order by " + spTo[1]
					mm := []map[string]any{}
					err := To(&mm).Query(q)
					if !lg.CheckError(err) {
						ress := []any{}
						for _, res := range mm {
							ress = append(ress, res[spTo[1]])
						}
						if len(ress) > 0 {
							mmfkeys[spFrom[1]] = ress
							mmfkeysModels[spFrom[1]] = mm
							for _, v := range mmfkeysModels[spFrom[1]] {
								for i, vv := range v {
									if vvStr, ok := vv.(string); ok {
										if len(vvStr) > 20 {
											v[i] = vvStr[:20] + "..."
										}
									}
								}
							}
						}
					} else {
						lg.ErrorC("error:", "q", q, "spTo", spTo)
					}
				}
			}
		}
	} else {
		idString = cols[0]
	}

	if dbMem != nil {
		data := map[string]any{
			"dbType":         dbMem.Dialect,
			"table":          model,
			"rows":           rows,
			"total":          total,
			"dbcolumns":      dbCols,
			"pk":             idString,
			"fkeys":          mmfkeys,
			"fkeysModels":    mmfkeysModels,
			"columnsOrdered": cols,
		}
		if t != nil {
			data["columns"] = t.ModelTypes
		} else {
			data["columns"] = dbCols
		}
		data["admin_path"] = adminPathNameGroup
		data["static_url"] = staticUrl
		c.Json(map[string]any{
			"success": data,
		})
	} else {
		lg.ErrorC("table not found", "table", model)
		c.Status(404).Json(map[string]any{
			"error": "table not found",
		})
	}
}

var AllModelsGet = func(c *ksmux.Context) {
	model := c.Param("model")
	if model == "" {
		c.Json(map[string]any{
			"error": "Error: No model given in params",
		})
		return
	}

	dbMem, _ := GetMemoryDatabase(defaultDB)
	if dbMem == nil {
		lg.ErrorC("unable to find db in mem", "db", defaultDB)
		dbMem = &databases[0]
	}
	idString := "id"
	var t *TableEntity
	for i, tt := range dbMem.Tables {
		if tt.Name == model {
			idString = tt.Pk
			t = &dbMem.Tables[i]
		}
	}

	rows, err := Table(model).Database(defaultDB).OrderBy("-" + idString).Limit(paginationPer).Page(1).All()
	if err != nil {
		rows, err = Table(model).Database(defaultDB).All()
		if err != nil {
			if err != ErrNoData {
				c.Status(404).Error("Unable to find this model")
				return
			}
		}
	}
	dbCols, cols := GetAllColumnsTypes(model)
	mmfkeysModels := map[string][]map[string]any{}
	mmfkeys := map[string][]any{}
	if t != nil {
		for _, fkey := range t.Fkeys {
			spFrom := strings.Split(fkey.FromTableField, ".")
			if len(spFrom) == 2 {
				spTo := strings.Split(fkey.ToTableField, ".")
				if len(spTo) == 2 {
					q := "select * from " + spTo[0] + " order by " + spTo[1]
					mm := []map[string]any{}
					err := To(&mm).Query(q)
					if !lg.CheckError(err) {
						ress := []any{}
						for _, res := range mm {
							ress = append(ress, res[spTo[1]])
						}
						if len(ress) > 0 {
							mmfkeys[spFrom[1]] = ress
							mmfkeysModels[spFrom[1]] = mm
							for _, v := range mmfkeysModels[spFrom[1]] {
								for i, vv := range v {
									if vvStr, ok := vv.(string); ok {
										if len(vvStr) > 20 {
											v[i] = vvStr[:20] + "..."
										}
									}
								}
							}
						}
					} else {
						lg.ErrorC("error:", "q", q, "spTo", spTo)
					}
				}
			}
		}
	} else {
		idString = cols[0]
	}

	if dbMem != nil {
		data := map[string]any{
			"dbType":         dbMem.Dialect,
			"table":          model,
			"rows":           rows,
			"dbcolumns":      dbCols,
			"pk":             idString,
			"fkeys":          mmfkeys,
			"fkeysModels":    mmfkeysModels,
			"columnsOrdered": cols,
		}
		if t != nil {
			data["columns"] = t.ModelTypes
		} else {
			data["columns"] = dbCols
		}
		data["admin_path"] = adminPathNameGroup
		data["static_url"] = staticUrl
		data["trace_enabled"] = defaultTracer.enabled
		data["terminal_enabled"] = terminalUIEnabled
		c.Html("admin/admin_single_table.html", data)
	} else {
		lg.ErrorC("table not found", "table", model)
		c.Status(404).Error("Unable to find this model")
	}
}

var AllModelsSearch = func(c *ksmux.Context) {
	model := c.Param("model")
	if model == "" {
		c.Json(map[string]any{
			"error": "Error: No model given in params",
		})
		return
	}

	body := c.BodyJson()

	blder := Table(model).Database(defaultDB)
	if query, ok := body["query"]; ok {
		if v, ok := query.(string); ok {
			if v != "" {
				blder.Where(v)
			}
		} else {
			c.Json(map[string]any{
				"error": "Error: No query given in body",
			})
			return
		}
	}

	oB := ""
	t, err := GetMemoryTable(model, defaultDB)
	if lg.CheckError(err) {
		c.Json(map[string]any{
			"error": err,
		})
		return
	}

	mmfkeysModels := map[string][]map[string]any{}
	mmfkeys := map[string][]any{}
	for _, fkey := range t.Fkeys {
		spFrom := strings.Split(fkey.FromTableField, ".")
		if len(spFrom) == 2 {
			spTo := strings.Split(fkey.ToTableField, ".")
			if len(spTo) == 2 {
				q := "select * from " + spTo[0] + " order by " + spTo[1]
				mm := []map[string]any{}
				err := To(&mm).Query(q)
				if !lg.CheckError(err) {
					ress := []any{}
					for _, res := range mm {
						ress = append(ress, res[spTo[1]])
					}
					if len(ress) > 0 {
						mmfkeys[spFrom[1]] = ress
						mmfkeysModels[spFrom[1]] = mm
						for _, v := range mmfkeysModels[spFrom[1]] {
							for i, vv := range v {
								if vvStr, ok := vv.(string); ok {
									if len(vvStr) > 20 {
										v[i] = vvStr[:20] + "..."
									}
								}
							}
						}
					}
				} else {
					lg.ErrorC("error:", "q", q, "spTo", spTo)
				}
			}
		}
	}

	if oB != "" {
		blder.OrderBy(oB)
	} else {
		blder.OrderBy("-" + t.Pk) // Default order by primary key desc
	}

	// Get page from request body
	pageNum := 1
	if v, ok := body["page_num"]; ok {
		if pn, ok := v.(string); ok {
			if p, err := strconv.Atoi(pn); err == nil {
				pageNum = p
			}
		}
	}
	blder.Limit(paginationPer).Page(pageNum)

	data, err := blder.All()
	if err != nil {
		if err != ErrNoData {
			c.Status(http.StatusBadRequest).Json(map[string]any{
				"error": err.Error(),
			})
			return
		}
		data = []map[string]any{}
	}

	// Get total count for pagination
	var total int64
	var totalRows []int64
	query := "SELECT COUNT(*) FROM " + model
	if v, ok := body["query"]; ok {
		if vStr, ok := v.(string); ok && vStr != "" {
			query += " WHERE " + vStr
		}
	}
	err = To(&totalRows).Query(query)
	if err == nil {
		total = totalRows[0]
	}

	c.Json(map[string]any{
		"table":       model,
		"rows":        data,
		"cols":        t.Columns,
		"types":       t.ModelTypes,
		"fkeys":       mmfkeys,
		"fkeysModels": mmfkeysModels,
		"total":       total,
	})
}

var BulkDeleteRowPost = func(c *ksmux.Context) {
	data := struct {
		Ids   []uint
		Table string
	}{}
	if lg.CheckError(c.BodyStruct(&data)) {
		c.Error("BAD REQUEST")
		return
	}
	idString := "id"
	t, err := GetMemoryTable(data.Table, defaultDB)
	if err != nil {
		c.Status(404).Json(map[string]any{
			"error": "table not found",
		})
		return
	}
	if t.Pk != "" && t.Pk != "id" {
		idString = t.Pk
	}
	_, err = Table(data.Table).Database(defaultDB).Where(idString+" IN (?)", data.Ids).Delete()
	if lg.CheckError(err) {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	c.Json(map[string]any{
		"success": "DELETED WITH SUCCESS",
		"ids":     data.Ids,
	})
}

var CreateModelView = func(c *ksmux.Context) {
	data, files := c.ParseMultipartForm()

	model := data["table"][0]
	m := map[string]any{}
	for key, val := range data {
		switch key {
		case "table":
			continue
		case "uuid":
			if v := m[key]; v == "" {
				m[key] = GenerateUUID()
			} else {
				m[key] = val[0]
			}
		case "password":
			hash, _ := argon.Hash(val[0])
			m[key] = hash
		case "email":
			if !IsValidEmail(val[0]) {
				c.Json(map[string]any{
					"error": "email not valid",
				})
				return
			}
			m[key] = val[0]
		default:
			if key != "" && val[0] != "" && val[0] != "null" {
				m[key] = val[0]
			}
		}
	}
	inserted, err := Table(model).Database(defaultDB).InsertR(m)
	if err != nil {
		lg.ErrorC("CreateModelView error", "err", err)
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}

	idString := "id"
	t, _ := GetMemoryTable(data["table"][0], defaultDB)
	if t.Pk != "" && t.Pk != "id" {
		idString = t.Pk
	}
	pathUploaded, formName, err := handleFilesUpload(files, data["table"][0], fmt.Sprintf("%v", inserted[idString]), c, idString)
	if err != nil {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	if len(pathUploaded) > 0 {
		inserted[formName[0]] = pathUploaded[0]
	}

	c.Json(map[string]any{
		"success":  "Done !",
		"inserted": inserted,
	})
}

var UpdateRowPost = func(c *ksmux.Context) {
	// parse the fkorm and get data values + files
	data, files := c.ParseMultipartForm()
	id := data["row_id"][0]
	idString := "id"
	t, _ := GetMemoryTable(data["table"][0], defaultDB)
	if t.Pk != "" && t.Pk != "id" {
		idString = t.Pk
	}
	_, _, err := handleFilesUpload(files, data["table"][0], id, c, idString)
	if err != nil {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}

	modelDB, err := Table(data["table"][0]).Database(defaultDB).Where(idString+" = ?", id).One()

	if err != nil {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}

	ignored := []string{idString, "file", "image", "photo", "img", "fichier", "row_id", "table"}
	toUpdate := map[string]any{}
	for key, val := range data {
		if !SliceContains(ignored, key) {
			if modelDB[key] == val[0] {
				// no changes for bool
				continue
			}
			if key == "password" || key == "pass" {
				hash, err := argon.Hash(val[0])
				if err != nil {
					c.Error("unable to hash pass")
					return
				}
				toUpdate["`"+key+"`"] = hash
			} else {
				toUpdate["`"+key+"`"] = val[0]
			}
		}
	}

	s := ""
	values := []any{}
	if len(toUpdate) > 0 {
		for col, v := range toUpdate {
			if s == "" {
				s += col + "= ?"
			} else {
				s += "," + col + "= ?"
			}
			values = append(values, v)
		}
	}
	if s != "" {
		_, err := Table(data["table"][0]).Database(defaultDB).Where(idString+" = ?", id).Set(s, values...)
		if err != nil {
			c.Status(http.StatusBadRequest).Json(map[string]any{
				"error": err.Error(),
			})
			return
		}
	}
	s = ""
	if len(files) > 0 {
		for f := range files {
			if s == "" {
				s += f
			} else {
				s += "," + f
			}
		}
	}
	if len(toUpdate) > 0 {
		for k := range toUpdate {
			if s == "" {
				s += k
			} else {
				s += "," + k
			}
		}
	}

	ret, err := Table(data["table"][0]).Database(defaultDB).Where(idString+" = ?", id).One()
	if err != nil {
		c.Status(500).Error("something wrong happened")
		return
	}

	c.Json(map[string]any{
		"success": ret,
	})
}

var TracingGetView = func(c *ksmux.Context) {
	c.Html("admin/admin_tracing.html", map[string]any{
		"admin_path":       adminPathNameGroup,
		"static_url":       staticUrl,
		"trace_enabled":    defaultTracer.enabled,
		"terminal_enabled": terminalUIEnabled,
	})
}

var TerminalGetView = func(c *ksmux.Context) {
	c.Html("admin/admin_terminal.html", map[string]any{
		"admin_path":       adminPathNameGroup,
		"static_url":       staticUrl,
		"trace_enabled":    defaultTracer.enabled,
		"terminal_enabled": terminalUIEnabled,
	})
}

// WebSocket endpoint for terminal
var TerminalExecute = func(c *ksmux.Context) {
	var req struct {
		Command string `json:"command"`
		Session string `json:"session"`
	}
	if err := c.BodyStruct(&req); err != nil {
		c.Json(map[string]any{"type": "error", "content": err.Error()})
		return
	}

	currentDir, ok := termsessions.Get(req.Session)
	if !ok {
		currentDir, _ = os.Getwd()
		termsessions.Set(req.Session, currentDir)
	}

	output, newDir := executeCommand(req.Command, currentDir)

	if newDir != "" {
		termsessions.Set(req.Session, newDir)
	}

	c.Json(map[string]any{
		"type":    "output",
		"content": output,
	})
}

var TerminalAutoComplete = func(c *ksmux.Context) {
	currentDir, _ := os.Getwd()
	input := c.QueryParam("input")
	parts := strings.Fields(input)

	if len(parts) == 0 {
		c.Json(CompletionResult{
			Type:        CommandCompletion,
			Completions: basicCommands,
			Input:       input,
		})
		return
	}

	command := parts[0]
	lastPart := parts[len(parts)-1]

	if len(parts) > 1 && strings.HasPrefix(lastPart, "-") {
		if flags, ok := commandFlags[command]; ok {
			matches := fuzzyMatch(lastPart, flags)
			c.Json(CompletionResult{
				Type:        FlagCompletion,
				Completions: matches,
				Input:       input,
			})
			return
		}
	}

	if strings.HasPrefix(lastPart, "$") {
		envVars := getEnvironmentVariables(lastPart[1:])
		c.Json(CompletionResult{
			Type:        EnvCompletion,
			Completions: envVars,
			Input:       input,
		})
		return
	}

	if len(parts) > 1 {
		completions := getPathCompletions(currentDir, lastPart)
		c.Json(CompletionResult{
			Type:        PathCompletion,
			Completions: completions,
			Input:       input,
		})
		return
	}

	matches := fuzzyMatch(command, basicCommands)
	c.Json(CompletionResult{
		Type:        CommandCompletion,
		Completions: matches,
		Input:       input,
	})
}

var GetTraces = func(c *ksmux.Context) {
	dbtraces := GetDBTraces()
	if len(dbtraces) > 0 {
		for _, t := range dbtraces {
			sp, _ := ksmux.StartSpan(context.Background(), t.Query)
			sp.SetTag("query", t.Query)
			sp.SetTag("args", fmt.Sprint(t.Args))
			if t.Database != "" {
				sp.SetTag("database", t.Database)
			}
			sp.SetTag("duration", t.Duration.String())
			sp.SetDuration(t.Duration)
			sp.SetError(t.Error)
			sp.End()
		}
		ClearDBTraces()
	}

	traces := ksmux.GetTraces()
	traceList := make([]map[string]interface{}, 0)
	for traceID, spans := range traces {
		spanList := make([]map[string]interface{}, 0)
		for _, span := range spans {
			errorMsg := ""
			if span.Error() != nil {
				errorMsg = span.Error().Error()
			}
			spanList = append(spanList, map[string]interface{}{
				"id":         span.SpanID(),
				"parentID":   span.ParentID(),
				"name":       span.Name(),
				"startTime":  span.StartTime(),
				"endTime":    span.EndTime(),
				"duration":   span.Duration().String(),
				"tags":       span.Tags(),
				"statusCode": span.StatusCode(),
				"error":      errorMsg,
			})
		}
		traceList = append(traceList, map[string]interface{}{
			"traceID": traceID,
			"spans":   spanList,
		})
	}
	c.Json(traceList)
}

var ClearTraces = func(c *ksmux.Context) {
	ksmux.ClearTraces()
	c.Success("traces cleared")
}

func handleFilesUpload(files map[string][]*multipart.FileHeader, model string, id string, c *ksmux.Context, pkKey string) (uploadedPath []string, formName []string, err error) {
	if len(files) > 0 {
		for key, val := range files {
			file, _ := val[0].Open()
			defer file.Close()
			uploadedImage, err := uploadMultipartFile(file, val[0].Filename, mediaDir+"/uploads/")
			if err != nil {
				return uploadedPath, formName, err
			}
			row, err := Table(model).Database(defaultDB).Where(pkKey+" = ?", id).One()
			if err != nil {
				return uploadedPath, formName, err
			}
			database_image, okDB := row[key]
			uploadedPath = append(uploadedPath, uploadedImage)
			formName = append(formName, key)
			if database_image == uploadedImage {
				return uploadedPath, formName, errors.New("uploadedImage is the same")
			} else {
				if v, ok := database_image.(string); ok || okDB {
					err := c.DeleteFile(v)
					if err != nil {
						//le fichier n'existe pas
						_, err := Table(model).Database(defaultDB).Where(pkKey+" = ?", id).Set(key+" = ?", uploadedImage)
						lg.CheckError(err)
						continue
					} else {
						//le fichier existe et donc supprimer
						_, err := Table(model).Database(defaultDB).Where(pkKey+" = ?", id).Set(key+" = ?", uploadedImage)
						lg.CheckError(err)
						continue
					}
				}
			}
		}
	}
	return uploadedPath, formName, nil
}

var DropTablePost = func(c *ksmux.Context) {
	data := c.BodyJson()
	if table, ok := data["table"]; ok && table != "" {
		if t, ok := data["table"].(string); ok {
			_, err := Table(t).Database(defaultDB).Drop()
			if lg.CheckError(err) {
				c.Status(http.StatusBadRequest).Json(map[string]any{
					"error": err.Error(),
				})
				return
			}
		} else {
			c.Status(http.StatusBadRequest).Json(map[string]any{
				"error": "expecting 'table' to be string",
			})
		}
	} else {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "missing 'table' in body request",
		})
	}
	c.Json(map[string]any{
		"success": fmt.Sprintf("table %s Deleted !", data["table"]),
	})
}

var ExportView = func(c *ksmux.Context) {
	table := c.Param("table")
	if table == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "no param table found",
		})
		return
	}
	data, err := Table(table).Database(defaultDB).All()
	lg.CheckError(err)

	data_bytes, err := json.Marshal(data)
	lg.CheckError(err)

	c.Download(data_bytes, table+".json")
}

var ExportCSVView = func(c *ksmux.Context) {
	table := c.Param("table")
	if table == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "no param table found",
		})
		return
	}
	data, err := Table(table).Database(defaultDB).All()
	lg.CheckError(err)
	var buff bytes.Buffer
	writer := csv.NewWriter(&buff)

	cols := []string{}
	tab, _ := GetMemoryTable(table, defaultDB)
	if len(tab.Columns) > 0 {
		cols = tab.Columns
	} else if len(data) > 0 {
		d := data[0]
		for k := range d {
			cols = append(cols, k)
		}
	}

	err = writer.Write(cols)
	lg.CheckError(err)
	for _, sd := range data {
		values := []string{}
		for _, k := range cols {
			switch vv := sd[k].(type) {
			case string:
				values = append(values, vv)
			case bool:
				if vv {
					values = append(values, "true")
				} else {
					values = append(values, "false")
				}
			case int:
				values = append(values, strconv.Itoa(vv))
			case int64:
				values = append(values, strconv.Itoa(int(vv)))
			case uint:
				values = append(values, strconv.Itoa(int(vv)))
			case time.Time:
				values = append(values, vv.String())
			default:
				values = append(values, fmt.Sprintf("%v", vv))
			}

		}
		err = writer.Write(values)
		lg.CheckError(err)
	}
	writer.Flush()
	c.Download(buff.Bytes(), table+".csv")
}

var ImportView = func(c *ksmux.Context) {
	// get table name
	table := c.Request.FormValue("table")
	if table == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "no table !",
		})
		return
	}
	t, err := GetMemoryTable(table, defaultDB)
	if lg.CheckError(err) {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	// upload file and return bytes of file
	fname, dataBytes, err := c.UploadFile("thefile", "backup", "json", "csv")
	if lg.CheckError(err) {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	isCsv := strings.HasSuffix(fname, ".csv")

	// get old data and backup
	modelsOld, _ := Table(table).Database(defaultDB).All()
	if len(modelsOld) > 0 {
		modelsOldBytes, err := json.Marshal(modelsOld)
		if !lg.CheckError(err) {
			_ = os.MkdirAll(mediaDir+"/backup/", 0770)
			dst, err := os.Create(mediaDir + "/backup/" + table + "-" + time.Now().Format("2006-01-02") + ".json")
			lg.CheckError(err)
			defer dst.Close()
			_, err = dst.Write(modelsOldBytes)
			lg.CheckError(err)
		}
	}

	// fill list_map
	list_map := []map[string]any{}
	if isCsv {
		reader := csv.NewReader(bytes.NewReader(dataBytes))
		lines, err := reader.ReadAll()
		if lg.CheckError(err) {
			c.Status(http.StatusBadRequest).Json(map[string]any{
				"error": err.Error(),
			})
			return
		}

		for _, values := range lines {
			m := map[string]any{}
			for i := range values {
				m[t.Columns[i]] = values[i]
			}
			list_map = append(list_map, m)
		}
	} else {
		err := json.Unmarshal(dataBytes, &list_map)
		if lg.CheckError(err) {
			c.Status(http.StatusBadRequest).Json(map[string]any{
				"error": err.Error(),
			})
			return
		}
	}

	// create models in database
	var retErr []error
	for _, m := range list_map {
		_, err = Table(table).Database(defaultDB).Insert(m)
		if err != nil {
			retErr = append(retErr, err)
		}
	}
	if len(retErr) > 0 {
		c.Json(map[string]any{
			"success": "some data could not be added, " + errors.Join(retErr...).Error(),
		})
		return
	}

	c.Json(map[string]any{
		"success": "Import Done , you can see uploaded backups at ./" + mediaDir + "/backup folder",
	})
}

var ManifestView = func(c *ksmux.Context) {
	if embededDashboard {
		f, err := staticAndTemplatesFS[0].ReadFile(staticDir + "/manifest.json")
		if err != nil {
			lg.ErrorC("cannot embed manifest.json", "err", err)
			return
		}
		c.ServeEmbededFile("application/json; charset=utf-8", f)
	} else {
		c.ServeFile("application/json; charset=utf-8", staticDir+"/manifest.json")
	}
}

var ServiceWorkerView = func(c *ksmux.Context) {
	if embededDashboard {
		f, err := staticAndTemplatesFS[0].ReadFile(staticDir + "/sw.js")
		if err != nil {
			lg.ErrorC("cannot embed sw.js", "err", err)
			return
		}
		c.ServeEmbededFile("application/javascript; charset=utf-8", f)
	} else {
		c.ServeFile("application/javascript; charset=utf-8", staticDir+"/sw.js")
	}
}

var RobotsTxtView = func(c *ksmux.Context) {
	c.ServeFile("text/plain; charset=utf-8", "."+staticUrl+"/robots.txt")
}

var OfflineView = func(c *ksmux.Context) {
	c.Text("<h1>YOUR ARE OFFLINE, check connection</h1>")
}

func statsNbRecords() string {
	allTables := GetAllTables(defaultDB)
	q := []string{}
	for _, t := range allTables {
		q = append(q, "SELECT '"+t+"' AS table_name,COUNT(*) AS count FROM "+t)
	}
	query := strings.Join(q, ` UNION ALL `)

	var results []struct {
		TableName string `db:"table_name"`
		Count     int    `db:"count"`
	}
	if err := To(&results).Query(query); lg.CheckError(err) {
		return "0"
	}
	count := 0
	for _, r := range results {
		count += r.Count
	}
	return strconv.Itoa(count)
}

func statsDbSize() string {
	size, err := GetDatabaseSize(defaultDB)
	if err != nil {
		lg.Error(err)
		size = "0 MB"
	}
	return size
}

type LogEntry struct {
	Type  string
	At    string
	Extra string
}

// Global atomic counter for requests
var totalRequests uint64

// GetTotalRequests returns the current total requests count
func GetTotalRequests() uint64 {
	return atomic.LoadUint64(&totalRequests)
}

func parseLogString(logStr string) LogEntry {
	// Handle empty string case
	if logStr == "" {
		return LogEntry{}
	}

	// Split the time from the end
	parts := strings.Split(logStr, "time=")
	timeStr := ""
	mainPart := logStr

	if len(parts) > 1 {
		timeStr = strings.TrimSpace(parts[1])
		mainPart = strings.TrimSpace(parts[0])
	}

	// Get the log type (ERRO, INFO, etc)
	logType := ""
	if len(mainPart) >= 4 {
		logType = strings.TrimSpace(mainPart[:4])
		mainPart = mainPart[4:]
	}

	// Clean up the type
	switch logType {
	case "ERRO":
		logType = "ERROR"
	case "INFO":
		logType = "INFO"
	case "WARN":
		logType = "WARNING"
	case "DEBU":
		logType = "DEBUG"
	case "FATA":
		logType = "FATAL"
	default:
		logType = "N/A"
	}

	return LogEntry{
		Type:  logType,
		At:    timeStr,
		Extra: strings.TrimSpace(mainPart),
	}
}

func reverseSlice[T any](slice []T) []T {
	new := make([]T, 0, len(slice))
	for i := len(slice) - 1; i >= 0; i-- {
		new = append(new, slice[i])
	}
	return new
}

// GetDatabaseSize returns the size of the database in GB or MB
func GetDatabaseSize(dbName string) (string, error) {
	db := databases[0] // default db
	for _, d := range databases {
		if d.Name == dbName {
			db = d
			break
		}
	}

	var size float64
	var err error

	switch db.Dialect {
	case "sqlite", "sqlite3":
		// For SQLite, get the file size
		info, err := os.Stat(dbName + ".sqlite3")
		if err != nil {
			return "0 MB", fmt.Errorf("error getting sqlite db size: %v", err)
		}
		size = float64(info.Size())

	case "postgres", "postgresql":
		// For PostgreSQL, query the pg_database_size function
		var sizeBytes int64
		query := `SELECT pg_database_size($1)`

		err = GetConnection().QueryRow(query, db.Name).Scan(&sizeBytes)
		if err != nil {
			return "0 MB", fmt.Errorf("error getting postgres db size: %v", err)
		}
		size = float64(sizeBytes)

	case "mysql", "mariadb":
		// For MySQL/MariaDB, query information_schema
		var sizeBytes int64
		query := `
			SELECT SUM(data_length + index_length) 
			FROM information_schema.TABLES 
			WHERE table_schema = ?`
		err = GetConnection().QueryRow(query, db.Name).Scan(&sizeBytes)
		if err != nil {
			return "0 MB", fmt.Errorf("error getting mysql db size: %v", err)
		}
		size = float64(sizeBytes)

	default:
		return "0 MB", fmt.Errorf("unsupported database dialect: %s", db.Dialect)
	}

	// Convert bytes to GB (1 GB = 1024^3 bytes)
	sizeGB := size / (1024 * 1024 * 1024)

	// If size is less than 1 GB, convert to MB
	if sizeGB < 1 {
		sizeMB := size / (1024 * 1024)
		return fmt.Sprintf("%.2f MB", sizeMB), nil
	}

	return fmt.Sprintf("%.2f GB", sizeGB), nil
}

func uploadMultipartFile(file multipart.File, filename string, outPath string, acceptedFormats ...string) (string, error) {
	//create destination file making sure the path is writeable.
	if outPath == "" {
		outPath = mediaDir + "/uploads/"
	} else {
		if !strings.HasSuffix(outPath, "/") {
			outPath += "/"
		}
	}
	err := os.MkdirAll(outPath, 0770)
	if err != nil {
		return "", err
	}

	l := []string{"jpg", "jpeg", "png", "json"}
	if len(acceptedFormats) > 0 {
		l = acceptedFormats
	}

	if strings.ContainsAny(filename, strings.Join(l, "")) {
		dst, err := os.Create(outPath + filename)
		if err != nil {
			return "", err
		}
		defer dst.Close()

		//copy the uploaded file to the destination file
		if _, err := io.Copy(dst, file); err != nil {
			return "", err
		} else {
			url := "/" + outPath + filename
			return url, nil
		}
	} else {
		return "", fmt.Errorf("not in allowed extensions 'jpg','jpeg','png','json' : %v", l)
	}
}

// TERMINAL

type CompletionType string

const (
	CommandCompletion CompletionType = "command"
	FlagCompletion    CompletionType = "flag"
	PathCompletion    CompletionType = "path"
	EnvCompletion     CompletionType = "env"
)

type CompletionResult struct {
	Type        CompletionType `json:"type"`
	Completions []string       `json:"completions"`
	Input       string         `json:"input"`
}

// Command flags map
var commandFlags = map[string][]string{
	"ls":    {"-l", "-a", "-h", "-r", "--help"},
	"grep":  {"-i", "-v", "-r", "-n", "--help"},
	"rm":    {"-r", "-f", "-i", "--help"},
	"cp":    {"-r", "-f", "-i", "--help"},
	"mkdir": {"-p", "--help"},
}

var basicCommands = []string{
	"ls", "cd", "pwd", "clear", "cls", "cat", "touch",
	"mkdir", "rmdir", "grep", "cp", "mv", "rm", "exit",
}

// Helper functions
// Helper functions
func fuzzyMatch(input string, candidates []string) []string {
	if input == "" {
		return candidates
	}

	matches := make([]string, 0)
	inputLower := strings.ToLower(input)

	for _, candidate := range candidates {
		if strings.Contains(strings.ToLower(candidate), inputLower) {
			matches = append(matches, candidate)
		}
	}
	return matches
}

func getEnvironmentVariables(prefix string) []string {
	vars := make([]string, 0)
	for _, env := range os.Environ() {
		if parts := strings.SplitN(env, "=", 2); len(parts) > 0 {
			if strings.HasPrefix(strings.ToLower(parts[0]), strings.ToLower(prefix)) {
				vars = append(vars, "$"+parts[0])
			}
		}
	}
	return vars
}

func getPathCompletions(baseDir, partial string) []string {
	searchDir := baseDir
	prefix := ""

	if filepath.IsAbs(partial) {
		searchDir = filepath.Dir(partial)
		prefix = filepath.Dir(partial) + string(filepath.Separator)
	} else if strings.Contains(partial, string(filepath.Separator)) {
		searchDir = filepath.Join(baseDir, filepath.Dir(partial))
		prefix = filepath.Dir(partial) + string(filepath.Separator)
	}

	files, err := os.ReadDir(searchDir)
	if err != nil {
		return nil
	}

	completions := make([]string, 0)
	for _, file := range files {
		name := prefix + file.Name()
		if strings.HasPrefix(strings.ToLower(name), strings.ToLower(partial)) {
			if file.IsDir() {
				name += string(filepath.Separator)
			}
			completions = append(completions, name)
		}
	}
	return completions
}

func executeCommand(command, currentDir string) (output, newDir string) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return "", ""
	}

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		switch strings.ToLower(strings.TrimSpace(parts[0])) {
		case "cd":
			if len(parts) < 2 {
				currentDir, _ = os.Getwd()
				return "", currentDir
			}
			newDir := parts[1]
			if !filepath.IsAbs(newDir) {
				newDir = filepath.Join(currentDir, newDir)
			}
			if fi, err := os.Stat(newDir); err == nil && fi.IsDir() {
				// Change directory and show contents
				currentDir = newDir
				// Execute ls command to show directory contents
				cmd = exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command",
					"[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Get-ChildItem")
				cmd.Dir = currentDir
				out, err := cmd.CombinedOutput()
				if err != nil {
					return "Error: " + err.Error() + "\n", currentDir
				}
				return string(out), currentDir
			}
			return "Error: Not a directory\n", ""

		case "ls":
			args := []string{"-NoProfile", "-NonInteractive", "-Command"}
			cmdStr := "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Get-ChildItem"

			// Handle directory argument
			if len(parts) > 1 {
				cmdStr += " -Path '" + parts[1] + "'"
			}

			args = append(args, cmdStr)
			cmd = exec.Command("powershell", args...)

		case "pwd":
			cmd = exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command",
				"[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; (Get-Location).Path")

		case "clear", "cls":
			return "CLEAR", ""

		case "exit", "quit":
			return "Closing session...\n", ""

		case "cp":
			if len(parts) >= 3 {
				cmd = exec.Command("cmd", "/c", "chcp 65001 >nul && copy", parts[1], parts[2])
			} else {
				return "Usage: cp source destination\n", ""
			}

		case "rm":
			if len(parts) >= 2 {
				cmd = exec.Command("cmd", "/c", "chcp 65001 >nul && del", parts[1])
			} else {
				return "Usage: rm filename\n", ""
			}

		case "mv":
			if len(parts) >= 3 {
				cmd = exec.Command("cmd", "/c", "chcp 65001 >nul && move", parts[1], parts[2])
			} else {
				return "Usage: mv source destination\n", ""
			}

		case "cat":
			if len(parts) < 2 {
				return "Usage: cat filename\n", ""
			}
			cmd = exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command",
				"[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Get-Content", parts[1])

		case "touch":
			if len(parts) < 2 {
				return "Usage: touch filename\n", ""
			}
			// Check if file exists
			filePath := filepath.Join(currentDir, parts[1])
			if _, err := os.Stat(filePath); err == nil {
				return "Error: File already exists\n", ""
			}
			cmd = exec.Command("cmd", "/c", "echo.>", parts[1])

		case "mkdir":
			if len(parts) < 2 {
				return "Usage: mkdir dirname\n", ""
			}
			cmd = exec.Command("cmd", "/c", "mkdir", parts[1])

		case "rmdir":
			if len(parts) < 2 {
				return "Usage: rmdir dirname\n", ""
			}
			cmd = exec.Command("cmd", "/c", "rmdir", "/s", "/q", parts[1])

		case "grep":
			if len(parts) < 2 {
				return "Usage: grep [flags] pattern [file...]\n", ""
			}

			args := []string{"/c", "findstr", "/l"}
			if len(parts) == 2 {
				// Simple pattern search
				pattern := strings.Trim(parts[1], "\"'") // Remove quotes if present
				args = append(args, "/n", "/i", pattern, "*.*")
			} else {
				pattern := ""
				files := []string{}

				for i := 1; i < len(parts); i++ {
					arg := parts[i]
					if strings.HasPrefix(arg, "-") {
						switch arg {
						case "-r", "-R":
							args = append(args, "/s")
						case "-n":
							args = append(args, "/n")
						case "-i":
							args = append(args, "/i")
						case "-v":
							args = append(args, "/v")
						}
					} else if pattern == "" {
						pattern = strings.Trim(arg, "\"'") // Remove quotes if present
					} else {
						files = append(files, arg)
					}
				}

				if pattern == "" {
					return "Error: No pattern specified\n", ""
				}

				if !strings.Contains(strings.Join(args, " "), "/n") {
					args = append(args, "/n")
				}

				args = append(args, pattern)
				if len(files) > 0 {
					args = append(args, files...)
				} else {
					args = append(args, "*.*")
				}
			}
			cmd = exec.Command("cmd", args...)
		default:
			// Handle any command with potential flags
			if strings.HasPrefix(parts[0], "go") || strings.HasPrefix(parts[0], "git") {
				// For commands like go, git - pass all args directly
				cmd = exec.Command(parts[0], parts[1:]...)
			} else {
				// For other Windows commands, preserve UTF-8 and pass all args
				args := []string{"/c", "chcp 65001 >nul &&", parts[0]}
				args = append(args, parts[1:]...)
				cmd = exec.Command("cmd", args...)
			}
		}
	} else {
		// Unix commands - always pass all arguments to preserve flags
		cmd = exec.Command(parts[0], parts[1:]...)
	}

	if cmd != nil {
		cmd.Dir = currentDir
		out, err := cmd.CombinedOutput()
		if err != nil {
			return "Error: " + err.Error() + "\n", ""
		}
		return string(out), ""
	}

	return "", ""
}
