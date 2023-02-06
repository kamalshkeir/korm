package korm

import (
	"embed"
	"os"
	"os/exec"
	"strings"
	"time"

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
	serverBus.App.NewFuncMap("jsTime", func(t any) string {
		valueToReturn := ""
		switch v := t.(type) {
		case time.Time:
			if !v.IsZero() {
				valueToReturn = v.Format("2006-01-02T15:04")
			} else {
				valueToReturn = time.Now().Format("2006-01-02T15:04")
			}
		case int:
			valueToReturn = time.Unix(int64(v), 0).Format("2006-01-02T15:04")
		case uint:
			valueToReturn = time.Unix(int64(v), 0).Format("2006-01-02T15:04")
		case int64:
			valueToReturn = time.Unix(v, 0).Format("2006-01-02T15:04")
		case string:
			if len(v) >= len("2006-01-02T15:04") && strings.Contains(v[:13], "T") {
				p, err := time.Parse("2006-01-02T15:04", v)
				if klog.CheckError(err) {
					valueToReturn = time.Now().Format("2006-01-02T15:04")
				} else {
					valueToReturn = p.Format("2006-01-02T15:04")
				}
			} else {
				if len(v) >= 16 {
					p, err := time.Parse("2006-01-02 15:04", v[:16])
					if klog.CheckError(err) {
						valueToReturn = time.Now().Format("2006-01-02T15:04")
					} else {
						valueToReturn = p.Format("2006-01-02T15:04")
					}
				}
			}
		default:
			if v != nil {
				klog.Printf("rdtype of %v %T is not handled,type is: %v\n", t, v, v)
			}
			valueToReturn = ""
		}
		return valueToReturn
	})
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
