package korm

import (
	"embed"
	"os"
	"os/exec"

	"github.com/kamalshkeir/klog"
)

func cloneAndMigrateDashboard(staticAndTemplatesEmbeded ...embed.FS) {
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
		serverBus.App.EmbededStatics(StaticDir, staticAndTemplatesEmbeded[0], "static")
		err := serverBus.App.EmbededTemplates(staticAndTemplatesEmbeded[1], "assets/templates")
		klog.CheckError(err)
	} else {
		serverBus.App.LocalStatics(StaticDir, "static")
		err := serverBus.App.LocalTemplates("assets/templates")
		klog.CheckError(err)
	}
	err := AutoMigrate[User]("users")
	if klog.CheckError(err) {
		return
	}
}
