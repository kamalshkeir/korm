package korm

import (
	"context"
	"embed"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/kamalshkeir/kmux"
)

func init() {
	kmux.BeforeRenderHtml(func(reqCtx context.Context, data *map[string]any) {
		const key kmux.ContextKey = "user"
		user, ok := reqCtx.Value(key).(User)
		if ok {
			(*data)["IsAuthenticated"] = true
			(*data)["User"] = user
		} else {
			(*data)["IsAuthenticated"] = false
			(*data)["User"] = nil
		}
	})
}

func initAdminUrlPatterns(r *kmux.Router, StaticAndTemplatesEmbeded ...embed.FS) {
	if EmbededDashboard && len(StaticAndTemplatesEmbeded) > 0 {
		r.EmbededStatics(StaticDir, StaticAndTemplatesEmbeded[0], "static")
	} else {
		r.LocalStatics(StaticDir, "static")
	}
	media_root := http.FileServer(http.Dir("./" + MediaDir))
	r.GET(`/`+MediaDir+`/*`, func(c *kmux.Context) {
		http.StripPrefix("/"+MediaDir+"/", media_root).ServeHTTP(c.ResponseWriter, c.Request)
	})
	r.GET("/mon/ping", func(c *kmux.Context) { c.Status(200).Text("pong") })
	r.GET("/offline", OfflineView)
	r.GET("/manifest.webmanifest", ManifestView)
	r.GET("/sw.js", ServiceWorkerView)
	r.GET("/robots.txt", RobotsTxtView)
	adminGroup := r.Group(AdminPathNameGroup)
	adminGroup.GET("/", Admin(IndexView))
	adminGroup.GET("/login", Auth(LoginView))
	adminGroup.POST("/login", Auth(LoginPOSTView))
	adminGroup.GET("/logout", LogoutView)
	adminGroup.POST("/delete/row", Admin(DeleteRowPost))
	adminGroup.POST("/update/row", Admin(UpdateRowPost))
	adminGroup.POST("/create/row", Admin(CreateModelView))
	adminGroup.POST("/drop/table", Admin(DropTablePost))
	adminGroup.GET("/table/model:str", Admin(AllModelsGet))
	adminGroup.POST("/table/model:str/search", Admin(AllModelsSearch))
	adminGroup.GET("/get/model:str/id:int", Admin(SingleModelGet))
	adminGroup.GET("/export/table:str", Admin(ExportView))
	adminGroup.POST("/import", Admin(ImportView))
	if Pprof {
		r.GET("/debug/*", func(c *kmux.Context) {
			if strings.Contains(c.Request.URL.Path, "profile") {
				pprof.Profile(c.ResponseWriter, c.Request)
				return
			} else if strings.Contains(c.Request.URL.Path, "trace") {
				pprof.Trace(c.ResponseWriter, c.Request)
				return
			}
			pprof.Index(c.ResponseWriter, c.Request)
		})
	}
}
