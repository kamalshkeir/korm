package korm

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmux"
)

var (
	basePath         = "/api"
	registeredTables = []string{}
	globalMiddws     []func(handler kmux.Handler) kmux.Handler
	tableMethods     = map[string]string{}
)

var ApiIndexHandler = func(c *kmux.Context) {
	m := map[string]TableEntity{}
	for _, t := range registeredTables {
		tb, _ := GetMemoryTable(t)
		m[t] = tb
	}
	if len(tableMethods) == 0 {
		klog.Printf("rderror: no method are allowed for the api\n")
		c.Text("error: no method are allowed for the api")
		return
	}
	if len(registeredTables) == 0 {
		klog.Printf("rderror: no registered tables\n")
		c.Text("error: no registered tables")
		return
	}
	c.Html("admin/api.html", map[string]any{
		"tables":    registeredTables,
		"tbMem":     m,
		"tbMethods": tableMethods,
		"EndWithSlash": func(str string) bool {
			q := []rune(str)
			return q[len(q)-1] == '/'
		},
	})
}

func WithAPI(rootPath string, middws ...func(handler kmux.Handler) kmux.Handler) error {
	if serverBus == nil {
		return fmt.Errorf("no server used, you can use korm.WithBus or korm.WithDashboard before executing ModelViewSet")
	}
	if rootPath != "" {
		basePath = rootPath
		if !strings.HasPrefix(basePath, "/") {
			basePath = "/" + basePath
		}
		if strings.HasSuffix(basePath, "/") {
			ln := len(basePath)
			basePath = basePath[:ln-1]
		}
	}
	app := serverBus.App
	ApiIndexHandler = wrapHandlerWithMiddlewares(ApiIndexHandler, middws...)
	if len(middws) > 0 {
		globalMiddws = middws
	}
	app.GET(basePath, ApiIndexHandler)
	return nil
}

type TableRegistration[T comparable] struct {
	TableName     string
	Middws        []func(handler kmux.Handler) kmux.Handler
	Methods       []string
	BuilderGetAll func(modelBuilder *BuilderS[T]) *BuilderS[T]
	BuilderGetOne func(modelBuilder *BuilderS[T]) *BuilderS[T]
}

func (tr *TableRegistration[T]) HaveMethod(method string) bool {
	for _, m := range tr.Methods {
		if strings.EqualFold(m, method) {
			return true
		}
	}
	return false
}

func RegisterTable[T comparable](table TableRegistration[T], gendocs ...bool) error {
	var tbName string
	if table.TableName != "" {
		tbName = table.TableName
	} else {
		tbName = getTableName[T]()
		if tbName == "" {
			return fmt.Errorf("table %v not registered, use korm.AutoMigrate before", *new(T))
		}
	}

	app := serverBus.App
	var apiAllModels = func(c *kmux.Context) {
		q := ModelTable[T](tbName)
		if table.BuilderGetAll != nil {
			q = table.BuilderGetAll(q)
		}
		rows, err := q.All()
		if err != nil {
			if err.Error() != "no data found" {
				c.Status(http.StatusBadRequest).Json(map[string]any{
					"error": err.Error(),
				})
				return
			}
		}
		c.JsonIndent(rows)
	}
	var singleModelGet = func(c *kmux.Context) {
		model := tbName
		id := c.Param("id")
		if id == "" {
			c.Json(map[string]any{
				"error": "No id given in path",
			})
			return
		}
		idString := "id"
		tb, err := GetMemoryTable(model)
		if err == nil {
			idString = tb.Pk
		}

		q := ModelTable[T](model).Where(idString+" = ?", id)
		if table.BuilderGetOne != nil {
			q = table.BuilderGetOne(q)
		}
		rows, err := q.All()
		if err != nil {
			if err.Error() != "no data found" {
				c.Status(http.StatusBadRequest).Json(map[string]any{
					"error": err.Error(),
				})
				return
			} else {
				c.Status(http.StatusBadRequest).Json(map[string]any{
					"error": "not found",
				})
				return
			}
		}
		c.JsonIndent(rows)
	}
	var singleModelPut = func(c *kmux.Context) {
		model := tbName
		id := c.Param("id")
		if id == "" {
			c.Json(map[string]any{
				"error": "No id given in path",
			})
			return
		}
		body := c.BodyJson()
		if len(body) == 0 {
			c.Json(map[string]any{
				"error": "Body is empty",
			})
			return
		}
		idString := "id"
		tb, err := GetMemoryTable(model)
		if err == nil {
			idString = tb.Pk
		}
		setStat := ""
		values := []any{}
		for k, v := range body {
			setStat += k + "=?,"
			values = append(values, v)
		}
		setStat = setStat[:len(setStat)-1]
		_, err = ModelTable[T](model).Where(idString+" = ?", id).Set(setStat, values...)
		if err != nil {
			if id == "" {
				c.Json(map[string]any{
					"error": err.Error(),
				})
				return
			}
		}
		c.Json(map[string]any{
			"success": model + " where " + idString + " = " + id + " updated",
		})
	}
	var modelCreate = func(c *kmux.Context) {
		model := tbName
		body := c.BodyJson()
		if len(body) == 0 {
			c.Json(map[string]any{
				"error": "Body is empty",
			})
			return
		}
		insertedId, err := Table(model).Insert(body)
		if err != nil {
			c.Json(map[string]any{
				"error": err.Error(),
			})
			return
		}
		msg := model + " inserted "
		if insertedId > 0 {
			msg += "with insertedId = " + strconv.Itoa(insertedId)
		}
		c.JsonIndent(map[string]any{
			"success": msg,
		})
	}
	var modelDelete = func(c *kmux.Context) {
		model := tbName
		id := c.Param("id")
		if id == "" {
			c.Json(map[string]any{
				"error": "No id given in path",
			})
			return
		}
		idString := "id"
		tb, err := GetMemoryTable(model)
		if err == nil {
			idString = tb.Pk
		}

		_, err = Table(model).Where(idString+" = ?", id).Delete()
		if err != nil {
			c.Json(map[string]any{
				"error": err.Error(),
			})
			return
		}
		c.JsonIndent(map[string]any{
			"success": model + " with " + idString + " = " + id + " deleted",
		})
	}

	if len(table.Middws) > 0 {
		apiAllModels = wrapHandlerWithMiddlewares(apiAllModels, table.Middws...)
	} else if len(globalMiddws) > 0 {
		apiAllModels = wrapHandlerWithMiddlewares(apiAllModels, globalMiddws...)
	}
	var modType string
	if docsUsed {
		modType = fmt.Sprintf("%T", *new(T))
		if modType == "" {
			return fmt.Errorf("could not find type of %T %v %s", *new(T), *new(T), modType)
		}
	}
	if len(table.Methods) > 0 {
		for _, meth := range table.Methods {
			switch meth {
			case "get", "GET":
				getallRoute := app.GET(basePath+"/"+tbName, apiAllModels)
				getsingleRoute := app.GET(basePath+"/"+tbName+"/:id", singleModelGet)
				if docsUsed && len(gendocs) == 1 && gendocs[0] {
					getallRoute.Out("200 {array} "+modType+" 'all rows'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Get all rows from " + tbName)
					getsingleRoute.In("id path int required 'Pk column'").Out("200 {object} "+modType+" 'user model'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Get single row from " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",get"
			case "post", "POST":
				postRoute := app.POST(basePath+"/"+tbName, modelCreate)
				if docsUsed && len(gendocs) == 1 && gendocs[0] {
					postRoute.In("thebody body " + modType + " required 'create model'").Out("200 {object} korm.DocsSuccess 'success message'").Tags(tbName).Summary("Create new row in " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",post"
			case "put", "PUT":
				putRoute := app.PUT(basePath+"/"+tbName+"/:id", singleModelPut)
				if docsUsed && len(gendocs) == 1 && gendocs[0] {
					putRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",put"
			case "patch", "PATCH":
				patchRoute := app.PATCH(basePath+"/"+tbName+"/:id", singleModelPut)
				if docsUsed && len(gendocs) == 1 && gendocs[0] {
					patchRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",patch"
			case "delete", "DELETE":
				deleteRoute := app.DELETE(basePath+"/"+tbName+"/:id", modelDelete)
				if docsUsed && len(gendocs) == 1 && gendocs[0] {
					deleteRoute.In("id path int required 'Pk column'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Delete a row from " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",delete"
			case "*":
				table.Methods = append(table.Methods, "get", "post", "put", "patch", "delete")
				postRoute := app.POST(basePath+"/"+tbName, modelCreate)
				getallRoute := app.GET(basePath+"/"+tbName, apiAllModels)
				getsingleRoute := app.GET(basePath+"/"+tbName+"/:id", singleModelGet)
				putRoute := app.PUT(basePath+"/"+tbName+"/:id", singleModelPut)
				patchRoute := app.PATCH(basePath+"/"+tbName+"/:id", singleModelPut)
				deleteRoute := app.DELETE(basePath+"/"+tbName+"/:id", modelDelete)
				if docsUsed && len(gendocs) == 1 && gendocs[0] {
					postRoute.In("thebody body " + modType + " required 'create model'").Out("200 {object} korm.DocsSuccess 'success message'").Tags(tbName).Summary("Create new row in " + tbName)
					getallRoute.Out("200 {array} "+modType+" 'all rows'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Get all rows from " + tbName)
					getsingleRoute.In("id path int required 'Pk column'").Out("200 {object} "+modType+" 'user model'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Get single row from " + tbName)
					putRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
					patchRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
					deleteRoute.In("id path int required 'Pk column'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Delete a row from " + tbName)
				}
				tableMethods[tbName] = strings.ToLower(strings.Join(table.Methods, ","))
				return nil
			}
		}
		registeredTables = append(registeredTables, tbName)
	} else {
		table.Methods = append(table.Methods, "get", "post", "put", "patch", "delete")
		postRoute := app.POST(basePath+"/"+tbName, modelCreate)
		getallRoute := app.GET(basePath+"/"+tbName, apiAllModels)
		getsingleRoute := app.GET(basePath+"/"+tbName+"/:id", singleModelGet)
		putRoute := app.PUT(basePath+"/"+tbName+"/:id", singleModelPut)
		patchRoute := app.PATCH(basePath+"/"+tbName+"/:id", singleModelPut)
		deleteRoute := app.DELETE(basePath+"/"+tbName+"/:id", modelDelete)
		if docsUsed && len(gendocs) == 1 && gendocs[0] {
			postRoute.In("thebody body " + modType + " required 'create model'").Out("200 {object} korm.DocsSuccess 'success message'").Tags(tbName).Summary("Create new row in " + tbName)
			getallRoute.Out("200 {array} "+modType+" 'all rows'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Get all rows from " + tbName)
			getsingleRoute.In("id path int required 'Pk column'").Out("200 {object} "+modType+" 'user model'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Get single row from " + tbName)
			putRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
			patchRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
			deleteRoute.In("id path int required 'Pk column'").Out("200 {object} korm.DocsSuccess 'success message'", "400 {object} korm.DocsError 'error message'").Tags(tbName).Summary("Delete a row from " + tbName)
		}
		registeredTables = append(registeredTables, tbName)
		tableMethods[tbName] = strings.ToLower(strings.Join(table.Methods, ","))
		return nil
	}

	return nil
}

func wrapHandlerWithMiddlewares(handler func(c *kmux.Context), middws ...func(handler kmux.Handler) kmux.Handler) func(c *kmux.Context) {
	found := false
	if len(middws) > 0 {
		handler = middws[0](handler)
		ptr1 := &middws[0]
		ptr2 := &Auth
		if ptr1 == ptr2 {
			found = true
		}
		for i := 1; i < len(middws); i++ {
			ptr1 = &middws[i]
			handler = middws[i](handler)
			if ptr1 == ptr2 {
				found = true
			}
		}
		if !found {
			handler = Auth(handler)
		}
	} else {
		handler = Auth(handler)
	}
	return handler
}
