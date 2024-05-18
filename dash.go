package korm

import (
	"embed"
	"os"
	"os/exec"

	"github.com/kamalshkeir/lg"
)

var staticAndTemplatesFS []embed.FS

func cloneAndMigrateDashboard(migrateUser bool, staticAndTemplatesEmbeded ...embed.FS) {
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
		lg.CheckError(serverBus.App.EmbededStatics(staticAndTemplatesEmbeded[0], staticDir, staticUrl))
		err := serverBus.App.EmbededTemplates(staticAndTemplatesEmbeded[1], templatesDir)
		lg.CheckError(err)
	} else {
		lg.CheckError(serverBus.App.LocalStatics(staticDir, staticUrl))
		err := serverBus.App.LocalTemplates(templatesDir)
		lg.CheckError(err)
	}
	isDashboardCloned = true
	if migrateUser {
		err := AutoMigrate[User]("users")
		if lg.CheckError(err) {
			return
		}
	}
}
