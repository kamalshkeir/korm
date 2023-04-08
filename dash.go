package korm

import (
	"embed"
	"os"
	"os/exec"

	"github.com/kamalshkeir/klog"
)

var staticAndTemplatesFS []embed.FS

func cloneAndMigrateDashboard(migrateUser bool, staticAndTemplatesEmbeded ...embed.FS) {
	if _, err := os.Stat(AssetsDir); err != nil && !EmbededDashboard {
		// if not generated
		cmd := exec.Command("git", "clone", "https://github.com/"+RepoUser+"/"+RepoName)
		err := cmd.Run()
		if klog.CheckError(err) {
			return
		}
		err = os.RemoveAll(RepoName + "/.git")
		if err != nil {
			klog.Printf("rdunable to delete %s/.git :%v \n", RepoName, err)
		}
		err = os.Rename(RepoName, AssetsDir)
		if err != nil {
			klog.Printf("rdunable to rename %s : %v \n", RepoName, err)
		}
		klog.Printfs("grdashboard assets cloned\n")
	}

	if len(staticAndTemplatesEmbeded) > 0 {
		staticAndTemplatesFS = staticAndTemplatesEmbeded
		serverBus.App.EmbededStatics(staticAndTemplatesEmbeded[0], StaticDir, StaticUrl)
		err := serverBus.App.EmbededTemplates(staticAndTemplatesEmbeded[1], TemplatesDir)
		klog.CheckError(err)
	} else {
		serverBus.App.LocalStatics(StaticDir, StaticUrl)
		err := serverBus.App.LocalTemplates(TemplatesDir)
		klog.CheckError(err)
	}
	dashboardCloned = true
	if migrateUser {
		err := AutoMigrate[User]("users")
		if klog.CheckError(err) {
			return
		}
	}
}
