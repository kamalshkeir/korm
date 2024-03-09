package korm

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/kamalshkeir/ksbus"
	"github.com/kamalshkeir/ksmux"
)

var (
	basePath         = "/api"
	registeredTables = []string{}
	globalMiddws     []func(handler ksmux.Handler) ksmux.Handler
	tableMethods     = map[string]string{}
)

var ApiIndexHandler = func(c *ksmux.Context) {
	m := map[string]TableEntity{}
	for _, t := range registeredTables {
		tb, _ := GetMemoryTable(t)
		m[t] = tb
	}
	if len(tableMethods) == 0 {
		c.Text("error: no method are allowed for the api")
		return
	}
	if len(registeredTables) == 0 {
		c.Text("error: no registered tables")
		return
	}
	c.Html("admin/api.html", map[string]any{
		"admin_path": adminPathNameGroup,
		"tables":     registeredTables,
		"tbMem":      m,
		"tbMethods":  tableMethods,
		"static_url": StaticUrl,
		"EndWithSlash": func(str string) bool {
			q := []rune(str)
			return q[len(q)-1] == '/'
		},
	})
}

func WithAPI(rootPath string, middws ...func(handler ksmux.Handler) ksmux.Handler) *ksbus.Server {
	if serverBus == nil {
		serverBus = WithBus()
	}
	if rootPath != "" {
		basePath = rootPath
		if basePath[0] != '/' {
			basePath = "/" + basePath
		}
		basePath = strings.TrimSuffix(basePath, "/")
	}
	app := serverBus.App
	ApiIndexHandler = wrapHandlerWithMiddlewares(ApiIndexHandler, middws...)
	if len(middws) > 0 {
		globalMiddws = middws
	}
	app.Get(basePath, ApiIndexHandler)
	return serverBus
}

type TableRegistration[T any] struct {
	TableName     string
	Middws        []func(handler ksmux.Handler) ksmux.Handler
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

func RegisterTable[T any](table TableRegistration[T], gendocs ...bool) error {
	var tbName string
	if table.TableName != "" {
		tbName = table.TableName
	} else {
		tbName = getTableName[T]()
		if tbName == "" {
			return fmt.Errorf("table %v not registered, use korm.AutoMigrate before", *new(T))
		}
	}
	if !IsDashboardCloned {
		cloneAndMigrateDashboard(false)
	}

	app := serverBus.App
	var apiAllModels = func(c *ksmux.Context) {
		q := ModelTable[T](tbName)
		if table.BuilderGetAll != nil {
			q = table.BuilderGetAll(q)
		}
		rows, err := q.All()
		if err != nil {
			if err != ErrNoData {
				c.Status(http.StatusBadRequest).Json(map[string]any{
					"error": err.Error(),
				})
				return
			}
		}
		c.JsonIndent(rows)
	}
	var singleModelGet = func(c *ksmux.Context) {
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
		rows, err := q.One()
		if err != nil {
			if err != ErrNoData {
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
	var singleModelPut = func(c *ksmux.Context) {
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
	var modelCreate = func(c *ksmux.Context) {
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
		msg := strings.TrimSuffix(model, "s") + " inserted "
		if insertedId > 0 {
			msg += "with id = " + strconv.Itoa(insertedId)
		}
		c.JsonIndent(map[string]any{
			"success": msg,
		})
	}
	var modelDelete = func(c *ksmux.Context) {
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
	if IsDocsUsed {
		modType = fmt.Sprintf("%T", *new(T))
		if modType == "" {
			return fmt.Errorf("could not find type of %T %v %s", *new(T), *new(T), modType)
		}
		if strings.Contains(modType, "korm") && strings.Contains(modType, "User") {
			modType = "ksmuxdocs.DocsUser"
		}
	}

	if len(table.Methods) > 0 {
		for _, meth := range table.Methods {
			switch meth {
			case "get", "GET":
				getallRoute := app.Get(basePath+"/"+tbName, apiAllModels)
				getsingleRoute := app.Get(basePath+"/"+tbName+"/:id", singleModelGet)
				if IsDocsUsed && len(gendocs) == 1 && gendocs[0] {
					getallRoute.Out("200 {array} "+modType+" 'all rows'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Get all rows from " + tbName)
					getsingleRoute.In("id path int required 'Pk column'").Out("200 {object} "+modType+" 'user model'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Get single row from " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",get"
			case "post", "POST":
				postRoute := app.Post(basePath+"/"+tbName, modelCreate)
				if IsDocsUsed && len(gendocs) == 1 && gendocs[0] {
					postRoute.In("thebody body " + modType + " required 'create model'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'").Tags(tbName).Summary("Create new row in " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",post"
			case "put", "PUT":
				putRoute := app.Put(basePath+"/"+tbName+"/:id", singleModelPut)
				if IsDocsUsed && len(gendocs) == 1 && gendocs[0] {
					putRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",put"
			case "patch", "PATCH":
				patchRoute := app.Patch(basePath+"/"+tbName+"/:id", singleModelPut)
				if IsDocsUsed && len(gendocs) == 1 && gendocs[0] {
					patchRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",patch"
			case "delete", "DELETE":
				deleteRoute := app.Delete(basePath+"/"+tbName+"/:id", modelDelete)
				if IsDocsUsed && len(gendocs) == 1 && gendocs[0] {
					deleteRoute.In("id path int required 'Pk column'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Delete a row from " + tbName)
				}
				tableMethods[tbName] = tableMethods[tbName] + ",delete"
			case "*":
				table.Methods = append(table.Methods, "get", "post", "put", "patch", "delete")
				postRoute := app.Post(basePath+"/"+tbName, modelCreate)
				getallRoute := app.Get(basePath+"/"+tbName, apiAllModels)
				getsingleRoute := app.Get(basePath+"/"+tbName+"/:id", singleModelGet)
				putRoute := app.Put(basePath+"/"+tbName+"/:id", singleModelPut)
				patchRoute := app.Patch(basePath+"/"+tbName+"/:id", singleModelPut)
				deleteRoute := app.Delete(basePath+"/"+tbName+"/:id", modelDelete)
				if IsDocsUsed && len(gendocs) == 1 && gendocs[0] {
					postRoute.In("thebody body " + modType + " required 'create model'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'").Tags(tbName).Summary("Create new row in " + tbName)
					getallRoute.Out("200 {array} "+modType+" 'all rows'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Get all rows from " + tbName)
					getsingleRoute.In("id path int required 'Pk column'").Out("200 {object} "+modType+" 'user model'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Get single row from " + tbName)
					putRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
					patchRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
					deleteRoute.In("id path int required 'Pk column'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Delete a row from " + tbName)
				}
				tableMethods[tbName] = strings.ToLower(strings.Join(table.Methods, ","))
				return nil
			}
		}
		registeredTables = append(registeredTables, tbName)
	} else {
		table.Methods = append(table.Methods, "get", "post", "put", "patch", "delete")
		postRoute := app.Post(basePath+"/"+tbName, modelCreate)
		getallRoute := app.Get(basePath+"/"+tbName, apiAllModels)
		getsingleRoute := app.Get(basePath+"/"+tbName+"/:id", singleModelGet)
		putRoute := app.Put(basePath+"/"+tbName+"/:id", singleModelPut)
		patchRoute := app.Patch(basePath+"/"+tbName+"/:id", singleModelPut)
		deleteRoute := app.Delete(basePath+"/"+tbName+"/:id", modelDelete)

		if IsDocsUsed && len(gendocs) == 1 && gendocs[0] {
			postRoute.In("thebody body " + modType + " required 'create model'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'").Tags(tbName).Summary("Create new row in " + tbName)
			getallRoute.Out("200 {array} "+modType+" 'all rows'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Get all rows from " + tbName)
			getsingleRoute.In("id path int required 'Pk column'").Out("200 {object} "+modType+" 'user model'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Get single row from " + tbName)
			putRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
			patchRoute.In("id path int required 'Pk column'", "thebody body "+modType+" required 'model to update'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Update a row from " + tbName)
			deleteRoute.In("id path int required 'Pk column'").Out("200 {object} ksmuxdocs.DocsSuccess 'success message'", "400 {object} ksmuxdocs.DocsError 'error message'").Tags(tbName).Summary("Delete a row from " + tbName)
		}
		registeredTables = append(registeredTables, tbName)
		tableMethods[tbName] = strings.ToLower(strings.Join(table.Methods, ","))
		return nil
	}

	return nil
}

func wrapHandlerWithMiddlewares(handler func(c *ksmux.Context), middws ...func(handler ksmux.Handler) ksmux.Handler) func(c *ksmux.Context) {
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
