package korm

import (
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/kamalshkeir/ksmux"
)

func init() {
	const kormKeyUser = "korm-user"
	ksmux.BeforeRenderHtml("korm-user", func(c *ksmux.Context, data *map[string]any) {
		user, ok := c.GetKey(kormKeyUser)
		if ok {
			(*data)["IsAuthenticated"] = true
			(*data)["User"] = user
		} else {
			(*data)["IsAuthenticated"] = false
			(*data)["User"] = nil
		}
	})

}

var withRequestCounter = false

func initAdminUrlPatterns(withReqCounter bool, r *ksmux.Router) {
	media_root := http.FileServer(http.Dir("./" + mediaDir))
	r.Get(`/`+mediaDir+`/*path`, func(c *ksmux.Context) {
		http.StripPrefix("/"+mediaDir+"/", media_root).ServeHTTP(c.ResponseWriter, c.Request)
	})

	if withReqCounter {
		withReqCounter = true
		// request counter middleware
		r.Use(func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Skip counting for static files, favicons etc
				path := r.URL.Path
				if !strings.HasPrefix(path, staticUrl) &&
					!strings.Contains(path, "/favicon") &&
					!strings.Contains(path, "/robots.txt") &&
					!strings.Contains(path, "/manifest.json") &&
					!strings.Contains(path, "/sw.js") {
					atomic.AddUint64(&totalRequests, 1)
				}

				h.ServeHTTP(w, r)
			})
		})
	}

	adminGroup := r.Group(adminPathNameGroup)
	adminGroup.Get("/", Admin(DashView))
	adminGroup.Get("/login", Auth(LoginView))
	adminGroup.Post("/login", Auth(LoginPOSTView))
	adminGroup.Get("/logout", LogoutView)
	adminGroup.Get("/tables", Admin(TablesView))
	adminGroup.Post("/tables/all/:model", Admin(TableGetAll))
	adminGroup.Get("/tables/:model", Admin(AllModelsGet))
	adminGroup.Post("/tables/:model/search", Admin(AllModelsSearch))
	adminGroup.Post("/delete/rows", Admin(BulkDeleteRowPost))
	adminGroup.Post("/update/row", Admin(UpdateRowPost))
	adminGroup.Post("/create/row", Admin(CreateModelView))
	adminGroup.Post("/drop/table", Admin(DropTablePost))
	adminGroup.Get("/export/:table", Admin(ExportView))
	adminGroup.Get("/export/:table/csv", Admin(ExportCSVView))
	adminGroup.Get("/logs", Admin(LogsView))
	adminGroup.Post("/import", Admin(ImportView))
}
