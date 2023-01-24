package korm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/kamalshkeir/kmux"
)

var registeredTables = []TableRegistration{}

type ModelApi struct {
	basePath     string
	globalMiddws []func(handler kmux.Handler) kmux.Handler
}

var ApiIndexHandler = func(c *kmux.Context) {
	m := map[string]TableEntity{}
	for _, t := range registeredTables {
		tb, _ := GetMemoryTable(t.TableName)
		m[t.TableName] = tb
	}
	c.Html("admin/api.html", map[string]any{
		"tables": registeredTables,
		"tbMem":  m,
	})
}

func WithAPI(rootPath string, middws ...func(handler kmux.Handler) kmux.Handler) (*ModelApi, error) {
	if serverBus == nil {
		return nil, fmt.Errorf("no server used, you can use korm.WithBus or korm.WithDashboard before executing ModelViewSet")
	}
	api := &ModelApi{
		basePath: "/api",
	}
	if rootPath != "" {
		api.basePath = rootPath
		if !strings.HasPrefix(api.basePath, "/") {
			api.basePath = "/" + api.basePath
		}
		if strings.HasSuffix(api.basePath, "/") {
			ln := len(api.basePath)
			api.basePath = api.basePath[:ln-1]
		}
	}
	app := serverBus.App
	app.NewFuncMap("json", func(data any) string {
		d, err := json.MarshalIndent(data, "", "\t")
		if err != nil {
			d = []byte("cannot marshal data")
		}
		return string(d)
	})
	ApiIndexHandler = wrapHandlerWithMiddlewares(ApiIndexHandler, middws...)
	if len(middws) > 0 {
		api.globalMiddws = middws
	}
	app.GET(api.basePath, ApiIndexHandler)
	return api, nil
}

type TableRegistration struct {
	TableName       string
	Middws          []func(handler kmux.Handler) kmux.Handler
	SelectedColumns []string
	Methods         []string
}

func (tr *TableRegistration) HaveMethod(method string) bool {
	for _, m := range tr.Methods {
		if strings.EqualFold(m, method) {
			return true
		}
	}
	return false
}

func (ma *ModelApi) RegisterTable(table TableRegistration) error {
	app := serverBus.App
	var apiAllModels = func(c *kmux.Context) {
		model := c.Param("model")
		if model == "" {
			c.Json(map[string]any{
				"error": "No model given in params",
			})
			return
		}
		q := Table(model)
		if len(table.SelectedColumns) > 0 {
			if table.SelectedColumns[0] != "*" && table.SelectedColumns[0] != "" {
				q.Select(table.SelectedColumns...)
			}
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
		model := c.Param("model")
		if model == "" {
			c.Json(map[string]any{
				"error": "No model given in path",
			})
			return
		}
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

		q := Table(model).Where(idString+" = ?", id)
		if len(table.SelectedColumns) > 0 {
			if table.SelectedColumns[0] != "*" && table.SelectedColumns[0] != "" {
				q.Select(table.SelectedColumns...)
			}
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
	var singleModelPut = func(c *kmux.Context) {
		model := c.Param("model")
		if model == "" {
			c.Json(map[string]any{
				"error": "No model given in path",
			})
			return
		}
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
		_, err = Table(model).Where(idString+" = ?", id).Set(setStat, values...)
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
		model := c.Param("model")
		if model == "" {
			c.Json(map[string]any{
				"error": "No model given in path",
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
		model := c.Param("model")
		if model == "" {
			c.Json(map[string]any{
				"error": "No model given in path",
			})
			return
		}
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
	} else if len(ma.globalMiddws) > 0 {
		apiAllModels = wrapHandlerWithMiddlewares(apiAllModels, ma.globalMiddws...)
	}

	if len(table.Methods) > 0 {
		for _, meth := range table.Methods {
			switch meth {
			case "get", "GET":
				app.GET(ma.basePath+"/model:str", apiAllModels)
				app.GET(ma.basePath+"/model:str/id:int", singleModelGet)
			case "post", "POST":
				app.POST(ma.basePath+"/model:str", modelCreate)
			case "put", "PUT":
				app.PUT(ma.basePath+"/model:str/id:int", singleModelPut)
			case "patch", "PATCH":
				app.PATCH(ma.basePath+"/model:str/id:int", singleModelPut)
			case "delete", "DELETE":
				app.DELETE(ma.basePath+"/model:str/id:int", modelDelete)
			case "*":
				table.Methods = append(table.Methods, "get", "post", "put", "patch", "delete")
				app.POST(ma.basePath+"/model:str", modelCreate)
				app.GET(ma.basePath+"/model:str", apiAllModels)
				app.GET(ma.basePath+"/model:str/id:int", singleModelGet)
				app.PUT(ma.basePath+"/model:str/id:int", singleModelPut)
				app.PATCH(ma.basePath+"/model:str/id:int", singleModelPut)
				app.DELETE(ma.basePath+"/model:str/id:int", modelDelete)
				registeredTables = append(registeredTables, table)
				return nil
			}
		}
	} else {
		table.Methods = append(table.Methods, "get", "post", "put", "patch", "delete")
		app.POST(ma.basePath+"/model:str", modelCreate)
		app.GET(ma.basePath+"/model:str", apiAllModels)
		app.GET(ma.basePath+"/model:str/id:int", singleModelGet)
		app.PUT(ma.basePath+"/model:str/id:int", singleModelPut)
		app.PATCH(ma.basePath+"/model:str/id:int", singleModelPut)
		app.DELETE(ma.basePath+"/model:str/id:int", modelDelete)
	}
	registeredTables = append(registeredTables, table)
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
