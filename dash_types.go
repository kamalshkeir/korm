package korm

import (
	"path"
	"strings"
	"time"
)

var (
	paginationPer      = 10
	embededDashboard   = false
	mediaDir           = "media"
	docsUrl            = "/docs"
	staticUrl          = "/static"
	assetsDir          = "assets"
	staticDir          = path.Join(assetsDir, "/", "static")
	templatesDir       = path.Join(assetsDir, "/", "templates")
	repoUser           = "kamalshkeir"
	repoName           = "korm-dash"
	adminPathNameGroup = "/admin"
	// Debug when true show extra useful logs for queries executed for migrations and queries statements
	Debug = false
	// FlushCacheEvery execute korm.FlushCache() every 10 min by default, you should not worry about it, but useful that you can change it
	FlushCacheEvery = 10 * time.Minute
	// MaxOpenConns set max open connections for db pool
	MaxOpenConns = 50
	// MaxIdleConns set max idle connections for db pool
	MaxIdleConns = 30
	// MaxLifetime set max lifetime for a connection in the db pool
	MaxLifetime = 30 * time.Minute
	// MaxIdleTime set max idletime for a connection in the db pool
	MaxIdleTime = 30 * time.Minute
)

type User struct {
	Id        int       `json:"id,omitempty" korm:"pk"`
	Uuid      string    `json:"uuid,omitempty" korm:"size:40;iunique"`
	Username  string    `json:"username,omitempty" korm:"size:40;iunique"`
	Email     string    `json:"email,omitempty" korm:"size:50;iunique"`
	Password  string    `json:"password,omitempty" korm:"size:150;default:''"`
	IsAdmin   bool      `json:"is_admin,omitempty" korm:"default:false"`
	Image     string    `json:"image,omitempty" korm:"size:100;default:''"`
	CreatedAt time.Time `json:"created_at,omitempty" korm:"now"`
}

// SetAdminPath set admin path, default '/admin'
func SetAdminPath(path string) {
	if !strings.HasPrefix(path, "/") {
		adminPathNameGroup = "/" + path
	} else {
		adminPathNameGroup = path
	}
}
