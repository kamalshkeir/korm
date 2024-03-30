package korm

import (
	"embed"
	"os"
	"os/exec"

	"github.com/kamalshkeir/lg"
)

var staticAndTemplatesFS []embed.FS

func cloneAndMigrateDashboard(migrateUser bool, staticAndTemplatesEmbeded ...embed.FS) {
	if _, err := os.Stat(AssetsDir); err != nil && !EmbededDashboard {
		// if not generated
		cmd := exec.Command("git", "clone", "https://github.com/"+RepoUser+"/"+RepoName)
		err := cmd.Run()
		if lg.CheckError(err) {
			return
		}
		err = os.RemoveAll(RepoName + "/.git")
		if err != nil {
			lg.ErrorC("unable to delete .git", "repo", RepoName, "err", err)
		}
		err = os.Rename(RepoName, AssetsDir)
		if err != nil {
			lg.ErrorC("unable to rename", "repo", RepoName, "err", err)
		}
		lg.Printfs("grdashboard assets cloned\n")
	}

	if len(staticAndTemplatesEmbeded) > 0 {
		staticAndTemplatesFS = staticAndTemplatesEmbeded
		serverBus.App.EmbededStatics(staticAndTemplatesEmbeded[0], StaticDir, StaticUrl)
		err := serverBus.App.EmbededTemplates(staticAndTemplatesEmbeded[1], TemplatesDir)
		lg.CheckError(err)
	} else {
		serverBus.App.LocalStatics(StaticDir, StaticUrl)
		err := serverBus.App.LocalTemplates(TemplatesDir)
		lg.CheckError(err)
	}
	IsDashboardCloned = true
	if migrateUser {
		err := AutoMigrate[User]("users")
		if lg.CheckError(err) {
			return
		}
	}
}
