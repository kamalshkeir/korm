package korm

import (
	"embed"
	"os"
	"os/exec"
	"sync"

	"github.com/kamalshkeir/lg"
)

var (
	staticAndTemplatesFS    []embed.FS
	statsFuncs              []StatsFunc
	realtimeDashboardHooksOnce sync.Once
)

func initDashboardRealtimeHooks() {
	realtimeDashboardHooksOnce.Do(func() {
		broadcast := func(hd HookData) {
			if serverBus != nil {
				payload := map[string]any{
					"table":     hd.Table,
					"operation": hd.Operation,
					"pk":        hd.Pk,
					"data":      hd.Data,
					"old":       hd.Old,
					"new":       hd.New,
				}
				serverBus.Publish("korm_db_dashboard_hooks", payload)
				serverBus.Publish("korm:table:"+hd.Table, payload)
				serverBus.Publish("korm:changes", payload)
			}
		}
		OnInsert(broadcast)
		OnSet(broadcast)
		OnDelete(broadcast)
		OnDrop(broadcast)
	})
}

type StatsFunc struct {
	Name string
	Func func() string
}

func cloneAndMigrateDashboard(staticAndTemplatesEmbeded ...embed.FS) {
	AddDashStats(StatsFunc{
		Name: "Total records",
		Func: statsNbRecords,
	})
	AddDashStats(StatsFunc{
		Name: "Database <span style='color:var(--theme-color)'>" + defaultDB + "</span> size",
		Func: statsDbSize,
	})
	if _, err := os.Stat(assetsDir); err != nil && !embededDashboard {
		// if not generated
		cmd := exec.Command("git", "clone", "https://github.com/"+repoUser+"/"+repoName)
		err := cmd.Run()
		if lg.CheckError(err) {
			return
		}
		err = os.RemoveAll(repoName + "/.git")
		if err != nil {
			lg.ErrorC("unable to delete .git", "repo", repoName, "err", err)
		}
		err = os.Rename(repoName, assetsDir)
		if err != nil {
			lg.ErrorC("unable to rename", "repo", repoName, "err", err)
		}
		lg.Printfs("grdashboard assets cloned\n")
	}

	if len(staticAndTemplatesEmbeded) > 0 {
		staticAndTemplatesFS = staticAndTemplatesEmbeded
		lg.CheckError(serverBus.App().EmbededStatics(staticAndTemplatesEmbeded[0], staticDir, staticUrl))
		err := serverBus.App().EmbededTemplates(staticAndTemplatesEmbeded[1], templatesDir)
		lg.CheckError(err)
	} else {
		lg.CheckError(serverBus.App().LocalStatics(staticDir, staticUrl))
		err := serverBus.App().LocalTemplates(templatesDir)
		lg.CheckError(err)
	}
	err := AutoMigrate[User]("users", defaultDB)
	if lg.CheckError(err) {
		return
	}
	_ = AddChangesTrigger("users", defaultDB)
	initDashboardRealtimeHooks()
	if kanbanUIEnabled {
		err = AutoMigrate[Board]("_boards", defaultDB)
		if lg.CheckError(err) {
			return
		}
		err = AutoMigrate[Task]("_tasks", defaultDB)
		if lg.CheckError(err) {
			return
		}
	}
}

func AddDashStats(fn ...StatsFunc) {
	statsFuncs = append(statsFuncs, fn...)
}

func GetStats() []StatsFunc {
	return statsFuncs
}
