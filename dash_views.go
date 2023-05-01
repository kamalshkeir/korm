package korm

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kamalshkeir/aes"
	"github.com/kamalshkeir/argon"
	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmux"
)

var IndexView = func(c *kmux.Context) {
	allTables := GetAllTables()
	c.Html("admin/admin_index.html", map[string]any{
		"tables": allTables,
	})
}

var LoginView = func(c *kmux.Context) {
	c.Html("admin/admin_login.html", nil)
}

var LoginPOSTView = func(c *kmux.Context) {
	requestData := c.BodyJson()
	email := requestData["email"]
	passRequest := requestData["password"]

	data, err := Table("users").Where("email = ?", email).One()
	if err != nil {
		c.Status(500).Json(map[string]any{
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
					klog.CheckError(err)
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

var LogoutView = func(c *kmux.Context) {
	c.DeleteCookie("session")
	c.Status(http.StatusTemporaryRedirect).Redirect("/")
}

var AllModelsGet = func(c *kmux.Context) {
	model := c.Param("model")
	if model == "" {
		c.Json(map[string]any{
			"error": "Error: No model given in params",
		})
		return
	}

	dbMem, _ := GetMemoryDatabase(defaultDB)
	if dbMem == nil {
		klog.Printf("rdunable to find db in mem %s\n", defaultDB)
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

	rows, err := Table(model).OrderBy("-" + idString).Limit(PaginationPer).Page(1).All()
	if err != nil {
		rows, err = Table(model).All()
		if err != nil {
			// usualy should not use error string because it divulge infkormation, but here only admin use it, so no worry
			if err != ErrNoData {
				klog.CheckError(err)
				c.TextHtml("<h1>Unable to find this model</h1>")
				return
			}
		}
	}
	mmfkeys := map[string][]any{}
	for _, fkey := range t.Fkeys {
		spFrom := strings.Split(fkey.FromTableField, ".")
		if len(spFrom) == 2 {
			spTo := strings.Split(fkey.ToTableField, ".")
			if len(spTo) == 2 {
				q := "select " + spTo[1] + " from " + spTo[0] + " order by " + spTo[1]
				mm := []map[string]any{}
				err := To(&mm).Query(q)
				if !klog.CheckError(err) {
					ress := []any{}
					for _, res := range mm {
						ress = append(ress, res[spTo[1]])
					}
					if len(ress) > 0 {
						mmfkeys[spFrom[1]] = ress
					}
				} else {
					klog.Printf("rd%s %v\n", q, spTo)
				}
			}
		}
	}
	dbCols := GetAllColumnsTypes(model)
	if dbMem != nil {
		c.Html("admin/admin_all_models.html", map[string]any{
			"dbType":         dbMem.Dialect,
			"model_name":     model,
			"rows":           rows,
			"columns":        t.ModelTypes,
			"dbcolumns":      dbCols,
			"pk":             idString,
			"fkeys":          mmfkeys,
			"columnsOrdered": t.Columns,
		})
	} else {
		klog.Printf("rdtable %s not found\n", model)
		c.TextHtml("<h1>Unable to find this model</h1>")
	}
}

var AllModelsSearch = func(c *kmux.Context) {
	model := c.Param("model")
	if model == "" {
		c.Json(map[string]any{
			"error": "Error: No model given in params",
		})
		return
	}

	body := c.BodyJson()

	blder := Table(model)
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
	t, _ := GetMemoryTable(model)
	if orderby, ok := body["orderby"]; ok {
		if v, ok := orderby.(string); ok && v != "" {
			oB = v
		} else {
			oB = "-" + t.Pk
		}
	} else {
		oB = "-" + t.Pk
	}
	blder.OrderBy(oB)
	if v, ok := body["page_num"]; ok && v != "" {
		if page, ok := v.(string); !ok {
			c.Status(http.StatusBadRequest).Json(map[string]any{
				"error": "expecting page_num to be a sring",
			})
			return
		} else {
			pagenum, err := strconv.Atoi(page)
			if err == nil {
				blder.Limit(PaginationPer).Page(pagenum)
			} else {
				c.Status(http.StatusBadRequest).Json(map[string]any{
					"error": err.Error(),
				})
				return
			}
		}
	} else {
		blder.Limit(PaginationPer).Page(1)
	}

	data, err := blder.All()
	if err != nil {
		c.Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	c.Json(map[string]any{
		"rows": data,
		"cols": t.Columns,
	})
}

var DeleteRowPost = func(c *kmux.Context) {
	data := c.BodyJson()
	if data["mission"] == "delete_row" {
		if model, ok := data["model_name"]; ok {
			if mm, ok := model.(string); ok {
				idString := "id"
				t, _ := GetMemoryTable(mm)
				if t.Pk != "" && t.Pk != "id" {
					idString = t.Pk
				}
				modelDB, err := Table(mm).Where(idString+" = ?", data["id"]).One()
				if klog.CheckError(err) {
					klog.Printf("rddata received DeleteRowPost:%v\n", data)
					c.Status(http.StatusBadRequest).Json(map[string]any{
						"error": err.Error(),
					})
					return
				}
				if val, ok := modelDB["image"]; ok {
					if vv, ok := val.(string); ok && vv != "" {
						_ = c.DeleteFile(vv)
					}
				}

				if idS, ok := data["id"].(string); ok {
					_, err = Table(mm).Where(idString+" = ?", idS).Delete()

					if err != nil {
						c.Status(http.StatusBadRequest).Json(map[string]any{
							"error": err.Error(),
						})
					} else {
						c.Json(map[string]any{
							"success": "Done !",
							"id":      data["id"],
						})
						return
					}
				}
			} else {
				c.Status(http.StatusBadRequest).Json(map[string]any{
					"error": "expecting model_name to be string",
				})
				return
			}
		} else {
			c.Status(http.StatusBadRequest).Json(map[string]any{
				"error": "no model_name found in request body",
			})
			return
		}
	}
}

var CreateModelView = func(c *kmux.Context) {
	parseErr := c.Request.ParseMultipartForm(int64(kmux.MultipartSize))
	if parseErr != nil {
		klog.Printf("rdParse error = %v\n", parseErr)
	}
	data := c.Request.Form

	defer func() {
		err := c.Request.MultipartForm.RemoveAll()
		klog.CheckError(err)
	}()

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
	_, err := Table(model).Insert(m)
	if err != nil {
		klog.Printf("rdCreateModelView error: %v\n", err)
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}

	c.Json(map[string]any{
		"success": "Done !",
	})
}

var SingleModelGet = func(c *kmux.Context) {
	model := c.Param("model")
	if model == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "param model not defined",
		})
		return
	}
	id := c.Param("id")
	if id == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "param id not defined",
		})
		return
	}
	idString := "id"
	t, _ := GetMemoryTable(model)
	if t.Pk != "" && t.Pk != "id" {
		idString = t.Pk
	}

	modelRow, err := Table(model).Where(idString+" = ?", id).One()
	if klog.CheckError(err) {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	dbCols := GetAllColumnsTypes(model)
	mmfkeys := map[string][]any{}
	for _, fkey := range t.Fkeys {
		spFrom := strings.Split(fkey.FromTableField, ".")
		if len(spFrom) == 2 {
			spTo := strings.Split(fkey.ToTableField, ".")
			if len(spTo) == 2 {
				q := "select " + spTo[1] + " from " + spTo[0] + " order by " + spTo[1]
				mm, err := Table(spTo[0]).QueryM(q)
				if !klog.CheckError(err) {
					ress := []any{}
					for _, res := range mm {
						ress = append(ress, res[spTo[1]])
					}
					if len(ress) > 0 {
						mmfkeys[spFrom[1]] = ress
					}
				}
			}
		}
	}
	c.Html("admin/admin_single_model.html", map[string]any{
		"model":      modelRow,
		"model_name": model,
		"id":         id,
		"fkeys":      mmfkeys,
		"columns":    t.ModelTypes,
		"dbcolumns":  dbCols,
		"pk":         t.Pk,
	})
}

var UpdateRowPost = func(c *kmux.Context) {
	// parse the fkorm and get data values + files
	data, files := c.ParseMultipartForm()
	id := data["row_id"][0]
	idString := "id"
	t, _ := GetMemoryTable(data["table"][0])
	if t.Pk != "" && t.Pk != "id" {
		idString = t.Pk
	}
	err := handleFilesUpload(files, data["table"][0], id, c, idString)
	if err != nil {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}

	modelDB, err := Table(data["table"][0]).Where(idString+" = ?", id).One()

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
			toUpdate[key] = val[0]
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
		_, err := Table(data["table"][0]).Where(idString+" = ?", id).Set(s, values...)
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
	c.Json(map[string]string{
		"success": s + " updated successfully",
	})
}

func handleFilesUpload(files map[string][]*multipart.FileHeader, model string, id string, c *kmux.Context, idString string) error {
	if len(files) > 0 {
		for key, val := range files {
			file, _ := val[0].Open()
			defer file.Close()
			uploadedImage, err := uploadMultipartFile(file, val[0].Filename, MediaDir+"/uploads/")
			if err != nil {
				return err
			}
			row, err := Table(model).Where(idString+" = ?", id).One()
			if err != nil {
				return err
			}
			database_image := row[key]

			if database_image == uploadedImage {
				return errors.New("uploadedImage is the same")
			} else {
				if v, ok := database_image.(string); ok {
					err := c.DeleteFile(v)
					if err != nil {
						//le fichier existe pas
						_, err := Table(model).Where(idString+" = ?", id).Set(key+" = ?", uploadedImage)
						klog.CheckError(err)
						continue
					} else {
						//le fichier existe et donc supprimer
						_, err := Table(model).Where(idString+" = ?", id).Set(key+" = ?", uploadedImage)
						klog.CheckError(err)
						continue
					}
				}
			}

		}
	}
	return nil
}

var DropTablePost = func(c *kmux.Context) {
	data := c.BodyJson()
	if table, ok := data["table"]; ok && table != "" {
		if t, ok := data["table"].(string); ok {
			_, err := Table(t).Drop()
			if klog.CheckError(err) {
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

var ExportView = func(c *kmux.Context) {
	table := c.Param("table")
	if table == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "no param table found",
		})
		return
	}
	data, err := Table(table).All()
	klog.CheckError(err)

	data_bytes, err := json.Marshal(data)
	klog.CheckError(err)

	c.Download(data_bytes, table+".json")
}

var ExportCSVView = func(c *kmux.Context) {
	table := c.Param("table")
	if table == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "no param table found",
		})
		return
	}
	data, err := Table(table).All()
	klog.CheckError(err)
	var buff bytes.Buffer
	writer := csv.NewWriter(&buff)

	cols := []string{}
	tab, _ := GetMemoryTable(table)
	if len(tab.Columns) > 0 {
		cols = tab.Columns
	} else if len(data) > 0 {
		d := data[0]
		for k := range d {
			cols = append(cols, k)
		}
	}

	err = writer.Write(cols)
	klog.CheckError(err)
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
		klog.CheckError(err)
	}
	writer.Flush()
	c.Download(buff.Bytes(), table+".csv")
}

var ImportView = func(c *kmux.Context) {
	// get table name
	table := c.Request.FormValue("table")
	if table == "" {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": "no table !",
		})
		return
	}
	t, err := GetMemoryTable(table)
	if klog.CheckError(err) {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	// upload file and return bytes of file
	fname, dataBytes, err := c.UploadFile("thefile", "backup", "json", "csv")
	if klog.CheckError(err) {
		c.Status(http.StatusBadRequest).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	isCsv := strings.HasSuffix(fname, ".csv")

	// get old data and backup
	modelsOld, _ := Table(table).All()
	if len(modelsOld) > 0 {
		modelsOldBytes, err := json.Marshal(modelsOld)
		if !klog.CheckError(err) {
			_ = os.MkdirAll(MediaDir+"/backup/", 0664)
			dst, err := os.Create(MediaDir + "/backup/" + table + "-" + time.Now().Format("2006-01-02") + ".json")
			klog.CheckError(err)
			defer dst.Close()
			_, err = dst.Write(modelsOldBytes)
			klog.CheckError(err)
		}
	}

	// fill list_map
	list_map := []map[string]any{}
	if isCsv {
		reader := csv.NewReader(bytes.NewReader(dataBytes))
		lines, err := reader.ReadAll()
		if klog.CheckError(err) {
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
		if klog.CheckError(err) {
			c.Status(http.StatusBadRequest).Json(map[string]any{
				"error": err.Error(),
			})
			return
		}
	}

	// create models in database
	var retErr []error
	for _, m := range list_map {
		_, err = Table(table).Insert(m)
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
		"success": "Import Done , you can see uploaded backups at ./" + MediaDir + "/backup folder",
	})
}

var ManifestView = func(c *kmux.Context) {
	if EmbededDashboard {
		f, err := staticAndTemplatesFS[0].ReadFile(StaticDir + "/manifest.json")
		if err != nil {
			klog.Printf("rdcannot embed manifest.json from static :%v\n", err)
			return
		}
		c.ServeEmbededFile("application/json; charset=utf-8", f)
	} else {
		c.ServeFile("application/json; charset=utf-8", StaticDir+"/manifest.json")
	}
}

var ServiceWorkerView = func(c *kmux.Context) {
	if EmbededDashboard {
		f, err := staticAndTemplatesFS[0].ReadFile(StaticDir + "/sw.js")
		if err != nil {
			klog.Printf("rdcannot embed sw.js from static %v\n", err)
			return
		}
		c.ServeEmbededFile("application/javascript; charset=utf-8", f)
	} else {
		c.ServeFile("application/javascript; charset=utf-8", StaticDir+"/sw.js")
	}
}

var RobotsTxtView = func(c *kmux.Context) {
	c.ServeFile("text/plain; charset=utf-8", "./static/robots.txt")
}

var OfflineView = func(c *kmux.Context) {
	c.Text("<h1>YOUR ARE OFFLINE, check connection</h1>")
}

func uploadMultipartFile(file multipart.File, filename string, outPath string, acceptedFormats ...string) (string, error) {
	//create destination file making sure the path is writeable.
	if outPath == "" {
		outPath = MediaDir + "/uploads/"
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
