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
		(*data)["admin_path"] = adminPathNameGroup
		(*data)["static_url"] = staticUrl
		(*data)["trace_enabled"] = defaultTracer.enabled
		(*data)["terminal_enabled"] = terminalUIEnabled
		(*data)["kanban_enabled"] = kanbanUIEnabled
		(*data)["nodemanager_enabled"] = nodeManager != nil
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
	r.Get("/mon/ping", func(c *ksmux.Context) { c.Status(200).Text("pong") })
	r.Get("/offline", OfflineView)
	r.Get("/manifest.webmanifest", ManifestView)
	r.Get("/sw.js", ServiceWorkerView)
	r.Get("/robots.txt", RobotsTxtView)
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
	adminGroup.Get("/logs/get", Admin(GetLogsView))
	adminGroup.Get("/metrics/get", Admin(GetMetricsView))
	adminGroup.Post("/import", Admin(ImportView))
	adminGroup.Get("/restart", Admin(RestartView))
	if defaultTracer.enabled {
		adminGroup.Get("/traces", Admin(TracingGetView))
		adminGroup.Get("/traces/get", Admin(GetTraces))
		adminGroup.Post("/traces/clear", Admin(ClearTraces))
	}
	if terminalUIEnabled {
		adminGroup.Get("/terminal", Admin(TerminalGetView))
		adminGroup.Post("/terminal/execute", Admin(TerminalExecute))
		adminGroup.Get("/terminal/complete", Admin(TerminalComplete))
	}
	if kanbanUIEnabled {
		adminGroup.Get("/kanbans", Admin(KanbanListView))
		adminGroup.Post("/kanbans/create", Admin(KanbanBoardCreate))
		adminGroup.Post("/kanbans/delete", Admin(KanbanBoardDelete))
		adminGroup.Post("/kanbans/tasks/create", Admin(KanbanTaskCreate))
		adminGroup.Post("/kanbans/tasks/move", Admin(KanbanTaskMove))
		adminGroup.Post("/kanbans/tasks/delete", Admin(KanbanTaskDelete))
		adminGroup.Get("/kanbans/:id", Admin(KanbanView))
	}
}
