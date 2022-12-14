package korm

import (
	"path"
	"time"
)

var (
	Pprof              = false
	PaginationPer      = 10
	EmbededDashboard   = false
	MediaDir           = "media"
	AssetsDir          = "assets"
	StaticDir          = path.Join(AssetsDir, "/", "static")
	TemplatesDir       = path.Join(AssetsDir, "/", "templates")
	RepoUser           = "kamalshkeir"
	RepoName           = "korm-dashboard"
	AdminPathNameGroup = "/admin"
	// Debug when true show extra useful logs for queries executed for migrations and queries statements
	Debug = false
	// FlushCacheEvery execute korm.FlushCache() every 10 min by default, you should not worry about it, but useful that you can change it
	FlushCacheEvery = 10 * time.Minute
	// MaxOpenConns set max open connections for db pool
	MaxOpenConns = 20
	// MaxIdleConns set max idle connections for db pool
	MaxIdleConns = 7
	// MaxLifetime set max lifetime for a connection in the db pool
	MaxLifetime = 1 * time.Hour
	// MaxIdleTime set max idletime for a connection in the db pool
	MaxIdleTime = 1 * time.Hour
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
