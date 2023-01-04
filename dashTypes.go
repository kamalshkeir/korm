package korm

import (
	"path"
	"time"
)

var (
	Pprof            = false
	PaginationPer    = 10
	EmbededDashboard = false
	MediaDir         = "media"
	AssetsDir        = "assets"
	StaticDir        = path.Join(AssetsDir, "/", "static")
	TemplatesDir     = path.Join(AssetsDir, "/", "templates")
	RepoUser         = "kamalshkeir"
	RepoName         = "korm-dashboard"
)

type User struct {
	Id        int       `json:"id,omitempty" korm:"pk"`
	Uuid      string    `json:"uuid,omitempty" korm:"size:40;iunique"`
	Email     string    `json:"email,omitempty" korm:"size:50;iunique"`
	Password  string    `json:"password,omitempty" korm:"size:150"`
	IsAdmin   bool      `json:"is_admin,omitempty" korm:"default:false"`
	Image     string    `json:"image,omitempty" korm:"size:100;default:''"`
	CreatedAt time.Time `json:"created_at,omitempty" korm:"now"`
}
