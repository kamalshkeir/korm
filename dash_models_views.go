package korm

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kamalshkeir/argon"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
	"github.com/kamalshkeir/ulid"
)

type tableResult struct {
	TableName string `db:"table_name"`
	Count     int    `db:"count"`
}

var TablesView = func(c *ksmux.Context) {
	allTables := GetAllTables(defaultDB)
	q := []string{}
	for _, t := range allTables {
		q = append(q, "SELECT '"+t+"' AS table_name,COUNT(*) AS count FROM "+t)
	}
	query := strings.Join(q, ` UNION ALL `)

	results, err := Select[tableResult]().Database(defaultDB).Query(query)
	if lg.CheckError(err) {
		c.Error("something wrong happened")
		return
	}

	entities := schemaEntities(defaultDB)
	summaries := tableSummaries(defaultDB, entities)
	relations := buildTableRelations(entities)

	c.Html("admin/admin_tables.html", map[string]any{
		"tables":    allTables,
		"results":   results,
		"summaries": summaries,
		"relations": relations,
		"cards":     buildTableCards(summaries, relations),
	})
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
	totalRows, err := Select[int64]().Database(defaultDB).Query("SELECT COUNT(*) FROM " + model)
	if err == nil && len(totalRows) > 0 {
		total = totalRows[0]
	}

	dbCols, cols := GetAllColumnsTypes(model, defaultDB)
	mmfkeysModels := map[string][]map[string]any{}
	mmfkeys := map[string][]any{}
	if t != nil {
		for _, fkey := range t.Fkeys {
			spFrom := strings.Split(fkey.FromTableField, ".")
			if len(spFrom) == 2 {
				spTo := strings.Split(fkey.ToTableField, ".")
				if len(spTo) == 2 {
					q := "select * from " + spTo[0] + " order by " + spTo[1]
					mm, err := Select[map[string]any]().Query(q)
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
										if len(vvStr) > TruncatePer {
											v[i] = vvStr[:TruncatePer] + "..."
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
		ccc := cols
		if t != nil {
			ccc = t.Columns
		}

		data := map[string]any{
			"dbType":         dbMem.Dialect,
			"table":          model,
			"rows":           rows,
			"total":          total,
			"dbcolumns":      dbCols,
			"pk":             idString,
			"fkeys":          mmfkeys,
			"fkeysModels":    mmfkeysModels,
			"columnsOrdered": ccc,
		}
		if t != nil {
			data["columns"] = t.ModelTypes
		} else {
			data["columns"] = dbCols
		}
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
	dbCols, cols := GetAllColumnsTypes(model, defaultDB)
	mmfkeysModels := map[string][]map[string]any{}
	mmfkeys := map[string][]any{}
	if t != nil {
		for _, fkey := range t.Fkeys {
			spFrom := strings.Split(fkey.FromTableField, ".")
			if len(spFrom) == 2 {
				spTo := strings.Split(fkey.ToTableField, ".")
				if len(spTo) == 2 {
					q := "select * from " + spTo[0] + " order by " + spTo[1]
					mm, err := Select[map[string]any]().Query(q)
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
										if len(vvStr) > TruncatePer {
											v[i] = vvStr[:TruncatePer] + "..."
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
			"relations":      buildTableRelations(schemaEntities(defaultDB))[model],
			"rowCount":       countRows(defaultDB, model),
		}
		if t != nil {
			data["columns"] = t.ModelTypes
		} else {
			data["columns"] = dbCols
		}
		c.Html("admin/admin_single_table.html", data)
	} else {
		lg.ErrorC("table not found", "table", model)
		c.Status(404).Error("Unable to find this model")
	}
}

// countRows returns the number of rows of a table, 0 on error.
func countRows(dbName, table string) int {
	rows, err := Select[int64]().Database(dbName).Query("SELECT COUNT(*) FROM " + table)
	if err != nil || len(rows) == 0 {
		return 0
	}
	return int(rows[0])
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
				mm, err := Select[map[string]any]().Query(q)
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
									if len(vvStr) > TruncatePer {
										v[i] = vvStr[:TruncatePer] + "..."
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
	query := "SELECT COUNT(*) FROM " + model
	if v, ok := body["query"]; ok {
		if vStr, ok := v.(string); ok && vStr != "" {
			query += " WHERE " + vStr
		}
	}
	totalRows, err := Select[int64]().Database(defaultDB).Query(query)
	if err == nil && len(totalRows) > 0 {
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
	rowsToDelete, _ := Table(data.Table).Database(defaultDB).Where(idString+" IN (?)", data.Ids).All()
	_, err = Table(data.Table).Database(defaultDB).Where(idString+" IN (?)", data.Ids).Delete()
	if lg.CheckError(err) {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}

	username := getContextUser(c)
	for _, r := range rowsToDelete {
		RecordAudit(defaultDB, data.Table, fmt.Sprint(r[idString]), "delete", r, nil, username)
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
		case "pk":
			continue
		default:
			if strings.Contains(key, "ulid") {
				if v := m[key]; v == "" {
					m[key] = ulid.Make().String()
				} else {
					m[key] = val[0]
				}
			} else if strings.Contains(key, "uuid") {
				if v := m[key]; v == "" {
					m[key] = GenerateUUID()
				} else {
					m[key] = val[0]
				}
			} else if key != "" && val[0] != "" && val[0] != "null" {
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

	createUsername := getContextUser(c)
	RecordAudit(defaultDB, data["table"][0], fmt.Sprint(inserted[idString]), "insert", nil, inserted, createUsername)

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
	db, _ := GetMemoryDatabase(defaultDB)
	var t TableEntity
	for _, tab := range db.Tables {
		if tab.Name == data["table"][0] {
			t = tab
		}
	}
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
	quote := "`"
	if db.Dialect == POSTGRES || db.Dialect == COCKROACH {
		quote = "\""
	}
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
				toUpdate[quote+key+quote] = hash
			} else {
				toUpdate[quote+key+quote] = val[0]
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

	updateUsername := getContextUser(c)
	RecordAudit(defaultDB, data["table"][0], id, "update", modelDB, ret, updateUsername)

	c.Json(map[string]any{
		"success": ret,
	})
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

var TableSeedPOST = func(c *ksmux.Context) {
	tableName := c.Param("model")
	if tableName == "" {
		c.Status(400).Json(map[string]any{"success": false, "error": "table parameter required"})
		return
	}

	body := c.BodyJson()
	count := 10
	if body != nil {
		if v, ok := body["count"].(float64); ok && v > 0 {
			count = int(v)
		} else if v, ok := body["count"].(int); ok && v > 0 {
			count = v
		}
	}
	if count > 100 {
		count = 100
	}

	colTypes, cols := GetAllColumnsTypes(tableName, defaultDB)
	if len(cols) == 0 {
		c.Status(404).Json(map[string]any{"success": false, "error": "table not found or has no columns"})
		return
	}

	pk := "id"
	if memT, err := GetMemoryTable(tableName, defaultDB); err == nil && memT.Pk != "" {
		pk = memT.Pk
	}

	// Find foreign keys to pre-fetch existing parent IDs
	entities := schemaEntities(defaultDB)
	relations := buildTableRelations(entities)
	fkTargets := map[string][]any{}
	if relList, ok := relations[tableName]; ok {
		for _, r := range relList {
			if r.Kind == RelationBelongsTo && r.Field != "" && r.Other != "" {
				parentRows, err := Table(r.Other).Database(defaultDB).Limit(20).All()
				if err == nil && len(parentRows) > 0 {
					pPk := "id"
					if pMem, pErr := GetMemoryTable(r.Other, defaultDB); pErr == nil && pMem.Pk != "" {
						pPk = pMem.Pk
					}
					vals := []any{}
					for _, pr := range parentRows {
						if v, exists := pr[pPk]; exists && v != nil {
							vals = append(vals, v)
						}
					}
					if len(vals) > 0 {
						fkTargets[r.Field] = vals
					}
				}
			}
		}
	}

	firstNames := []string{"Alex", "Sam", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Cameron", "Dakota", "Reese"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	adjectives := []string{"bright", "swift", "silent", "brave", "calm", "clever", "gentle", "happy", "noble", "vivid"}
	cities := []string{"Paris", "Tokyo", "London", "Berlin", "Sydney", "Toronto", "Madrid", "Rome", "Seoul", "Austin"}

	insertedCount := 0
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < count; i++ {
		row := make(map[string]any)
		firstName := firstNames[r.Intn(len(firstNames))]
		lastName := lastNames[r.Intn(len(lastNames))]
		adj := adjectives[r.Intn(len(adjectives))]
		city := cities[r.Intn(len(cities))]

		for _, col := range cols {
			colLower := strings.ToLower(col)
			cType := strings.ToUpper(colTypes[col])

			// Skip auto-increment integer PK
			if col == pk {
				if strings.Contains(cType, "INT") || cType == "" {
					continue
				}
				// If string PK, generate UUID/ULID
				if strings.Contains(colLower, "ulid") {
					row[col] = ulid.Make().String()
				} else {
					row[col] = GenerateUUID()
				}
				continue
			}

			// Check foreign key
			if parentVals, hasFK := fkTargets[col]; hasFK && len(parentVals) > 0 {
				row[col] = parentVals[r.Intn(len(parentVals))]
				continue
			}

			// Smart mock generation by column name
			if strings.Contains(colLower, "email") {
				row[col] = fmt.Sprintf("%s.%s%d@example.com", strings.ToLower(firstName), strings.ToLower(lastName), r.Intn(999))
			} else if strings.Contains(colLower, "uuid") {
				row[col] = GenerateUUID()
			} else if strings.Contains(colLower, "ulid") {
				row[col] = ulid.Make().String()
			} else if strings.Contains(colLower, "password") {
				hash, _ := argon.Hash("secret123")
				row[col] = hash
			} else if strings.Contains(colLower, "username") || strings.Contains(colLower, "login") {
				row[col] = fmt.Sprintf("%s_%s%d", strings.ToLower(adj), strings.ToLower(firstName), r.Intn(99))
			} else if colLower == "first_name" || colLower == "firstname" {
				row[col] = firstName
			} else if colLower == "last_name" || colLower == "lastname" {
				row[col] = lastName
			} else if colLower == "name" || colLower == "full_name" {
				row[col] = fmt.Sprintf("%s %s", firstName, lastName)
			} else if strings.Contains(colLower, "city") {
				row[col] = city
			} else if strings.Contains(colLower, "phone") || strings.Contains(colLower, "tel") {
				row[col] = fmt.Sprintf("+336%08d", r.Intn(90000000)+10000000)
			} else if strings.Contains(colLower, "url") || strings.Contains(colLower, "website") {
				row[col] = fmt.Sprintf("https://%s-%s.io", strings.ToLower(adj), strings.ToLower(city))
			} else if strings.Contains(colLower, "image") || strings.Contains(colLower, "avatar") || strings.Contains(colLower, "photo") {
				row[col] = fmt.Sprintf("https://images.unsplash.com/photo-1534528741775-53994a69daeb?w=150&auto=format&fit=crop&q=80&seed=%d", r.Intn(1000))
			} else if strings.Contains(colLower, "lat") || strings.Contains(colLower, "latitude") {
				row[col] = 48.85 + (r.Float64()-0.5)*0.2
			} else if strings.Contains(colLower, "lng") || strings.Contains(colLower, "long") || strings.Contains(colLower, "longitude") {
				row[col] = 2.35 + (r.Float64()-0.5)*0.2
			} else if strings.HasPrefix(colLower, "is_") || strings.HasPrefix(colLower, "has_") || colLower == "active" || colLower == "enabled" || colLower == "status_bool" {
				if strings.Contains(cType, "INT") {
					row[col] = r.Intn(2)
				} else {
					row[col] = r.Intn(2) == 1
				}
			} else if strings.Contains(colLower, "status") {
				statuses := []string{"active", "pending", "completed", "archived"}
				row[col] = statuses[r.Intn(len(statuses))]
			} else if strings.Contains(colLower, "role") {
				roles := []string{"admin", "member", "editor"}
				row[col] = roles[r.Intn(len(roles))]
			} else if strings.Contains(colLower, "price") || strings.Contains(colLower, "amount") || strings.Contains(colLower, "total") || strings.Contains(colLower, "salary") {
				row[col] = float64(r.Intn(50000)+100) / 100.0
			} else if strings.Contains(colLower, "age") {
				row[col] = r.Intn(60) + 18
			} else if strings.Contains(colLower, "count") || strings.Contains(colLower, "quantity") || strings.Contains(colLower, "stock") {
				row[col] = r.Intn(150) + 1
			} else if strings.Contains(colLower, "title") {
				titles := []string{"Quarterly Growth Report", "Platform Performance Audit", "Security Assessment", "Customer Feedback Survey", "Design System Guidelines"}
				row[col] = titles[r.Intn(len(titles))]
			} else if strings.Contains(colLower, "desc") || strings.Contains(colLower, "content") || strings.Contains(colLower, "bio") || strings.Contains(colLower, "note") {
				row[col] = fmt.Sprintf("Automated sample entry for %s created during testing at %s.", col, time.Now().Format("15:04:05"))
			} else if strings.Contains(colLower, "created_at") || strings.Contains(colLower, "updated_at") || strings.Contains(colLower, "timestamp") || strings.Contains(colLower, "date") {
				if strings.Contains(cType, "INT") {
					row[col] = time.Now().Unix() - int64(r.Intn(86400*30))
				} else {
					row[col] = time.Now().Add(-time.Duration(r.Intn(720)) * time.Hour).Format("2006-01-02 15:04:05")
				}
			} else {
				// Fallback to type
				if strings.Contains(cType, "INT") {
					row[col] = r.Intn(1000) + 1
				} else if strings.Contains(cType, "DECIMAL") || strings.Contains(cType, "FLOAT") || strings.Contains(cType, "DOUBLE") || strings.Contains(cType, "REAL") {
					row[col] = float64(r.Intn(10000)) / 100.0
				} else if strings.Contains(cType, "BOOL") {
					row[col] = r.Intn(2) == 1
				} else {
					row[col] = fmt.Sprintf("sample_%s_%d", colLower, r.Intn(9999))
				}
			}
		}

		insId, err := Table(tableName).Database(defaultDB).Insert(row)
		if err == nil {
			insertedCount++
			RecordAudit(defaultDB, tableName, fmt.Sprint(insId), "insert", nil, row, "mock-seeder")
		} else {
			lg.ErrorC("unable to insert seed row", "err", err, "row", row)
		}
	}

	c.Json(map[string]any{
		"success":  true,
		"inserted": insertedCount,
		"table":    tableName,
	})
}
