package korm

import (
	"context"
	"embed"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/kamalshkeir/aes"
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
	if EmbededDashboard {
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
	adminGroup := r.Group("/admin")
	adminGroup.GET("", Admin(IndexView))
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

var Auth = func(handler kmux.Handler) kmux.Handler {
	const key kmux.ContextKey = "user"
	return func(c *kmux.Context) {
		session, err := c.GetCookie("session")
		if err != nil || session == "" {
			// NOT AUTHENTICATED
			c.DeleteCookie("session")
			handler(c)
			return
		}
		session, err = aes.Decrypt(session)
		if err != nil {
			handler(c)
			return
		}
		// Check session
		user, err := Model[User]().Where("uuid = ?", session).One()
		if err != nil {
			// session fail
			handler(c)
			return
		}

		// AUTHENTICATED AND FOUND IN DB
		ctx := context.WithValue(c.Request.Context(), key, user)
		*c = kmux.Context{
			Params:         c.ParamsMap(),
			Request:        c.Request.WithContext(ctx),
			ResponseWriter: c.ResponseWriter,
		}
		handler(c)
	}
}

var Admin = func(handler kmux.Handler) kmux.Handler {
	const key kmux.ContextKey = "user"
	return func(c *kmux.Context) {
		session, err := c.GetCookie("session")
		if err != nil || session == "" {
			// NOT AUTHENTICATED
			c.DeleteCookie("session")
			c.Status(http.StatusTemporaryRedirect).Redirect("/admin/login")
			return
		}
		session, err = aes.Decrypt(session)
		if err != nil {
			c.Status(http.StatusTemporaryRedirect).Redirect("/admin/login")
			return
		}
		user, err := Model[User]().Where("uuid = ?", session).One()

		if err != nil {
			// AUTHENTICATED BUT NOT FOUND IN DB
			c.Status(http.StatusTemporaryRedirect).Redirect("/admin/login")
			return
		}

		// Not admin
		if !user.IsAdmin {
			c.Status(403).Text("Middleware : Not allowed to access this page")
			return
		}

		ctx := context.WithValue(c.Request.Context(), key, user)
		*c = kmux.Context{
			Params:         c.ParamsMap(),
			Request:        c.Request.WithContext(ctx),
			ResponseWriter: c.ResponseWriter,
		}

		handler(c)
	}
}
