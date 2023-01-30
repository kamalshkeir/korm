package korm

import (
	"encoding/json"
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
	app.NewFuncMap("json", func(data any) string {
		d, err := json.MarshalIndent(data, "", "\t")
		if err != nil {
			d = []byte("cannot marshal data")
		}
		return string(d)
	})
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

func RegisterTable[T comparable](table TableRegistration[T]) error {
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
		model := c.Param("model")
		if model == "" {
			c.Json(map[string]any{
				"error": "No model given in params",
			})
			return
		}
		q := ModelTable[T](model)
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
	} else if len(globalMiddws) > 0 {
		apiAllModels = wrapHandlerWithMiddlewares(apiAllModels, globalMiddws...)
	}

	if len(table.Methods) > 0 {
		for _, meth := range table.Methods {
			switch meth {
			case "get", "GET":
				app.GET(basePath+"/model:str", apiAllModels)
				app.GET(basePath+"/model:str/id:int", singleModelGet)
				tableMethods[tbName] = tableMethods[tbName] + ",get"
			case "post", "POST":
				app.POST(basePath+"/model:str", modelCreate)
				tableMethods[tbName] = tableMethods[tbName] + ",post"
			case "put", "PUT":
				app.PUT(basePath+"/model:str/id:int", singleModelPut)
				tableMethods[tbName] = tableMethods[tbName] + ",put"
			case "patch", "PATCH":
				app.PATCH(basePath+"/model:str/id:int", singleModelPut)
				tableMethods[tbName] = tableMethods[tbName] + ",patch"
			case "delete", "DELETE":
				app.DELETE(basePath+"/model:str/id:int", modelDelete)
				tableMethods[tbName] = tableMethods[tbName] + ",delete"
			case "*":
				table.Methods = append(table.Methods, "get", "post", "put", "patch", "delete")
				app.POST(basePath+"/model:str", modelCreate)
				app.GET(basePath+"/model:str", apiAllModels)
				app.GET(basePath+"/model:str/id:int", singleModelGet)
				app.PUT(basePath+"/model:str/id:int", singleModelPut)
				app.PATCH(basePath+"/model:str/id:int", singleModelPut)
				app.DELETE(basePath+"/model:str/id:int", modelDelete)
				registeredTables = append(registeredTables, tbName)
				tableMethods[tbName] = strings.ToLower(strings.Join(table.Methods, ","))
				return nil
			}
		}
		registeredTables = append(registeredTables, tbName)
	} else {
		table.Methods = append(table.Methods, "get", "post", "put", "patch", "delete")
		app.POST(basePath+"/model:str", modelCreate)
		app.GET(basePath+"/model:str", apiAllModels)
		app.GET(basePath+"/model:str/id:int", singleModelGet)
		app.PUT(basePath+"/model:str/id:int", singleModelPut)
		app.PATCH(basePath+"/model:str/id:int", singleModelPut)
		app.DELETE(basePath+"/model:str/id:int", modelDelete)
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
