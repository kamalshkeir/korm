package main

import (
	"time"
)

type TestTable struct {
	Id   int `korm:"pk"`
	Name string
}

type Another struct {
	Id        int `korm:"pk"`
	Name      string
	UserID    uint   `korm:"fk:users.id:cascade:cascade"`
	Content   string `korm:"text"`
	IsAdmin   bool
	CreatedAt time.Time `korm:"now"`
	UpdatedAt time.Time `korm:"update"`
}

// func main() {
// 	aes.SetSecret("blablabla")
// 	if err := korm.New(korm.SQLITE, "db", sqlitedriver.Use()); lg.CheckError(err) {
// 		return
// 	}
// 	defer korm.Shutdown()

// 	err := korm.AutoMigrate[TestTable]("test_table")
// 	lg.CheckError(err)
// 	err = korm.AutoMigrate[Another]("another")
// 	lg.CheckError(err)
// 	srv := korm.WithDashboard(":9313", korm.DashOpts{
// 		WithTracing: true,
// 	})
// 	korm.WithShell()

// 	srv.App.Get("/", func(c *ksmux.Context) {
// 		c.Json(map[string]any{
// 			"users": "ok",
// 		})
// 	})
// 	srv.App.Get("/test", func(c *ksmux.Context) {
// 		sp, ctx := ksmux.StartSpan(c.Request.Context(), "Test Handler")
// 		defer sp.End()
// 		sp.SetTag("bla", "value1")
// 		users, err := korm.Model[korm.User]().Trace().Where("id > ?", 0).All()
// 		if lg.CheckError(err) {
// 			c.SetStatus(500)
// 			return
// 		}
// 		doWork(ctx, 1)
// 		doWork(ctx, 2)
// 		c.Json(map[string]any{
// 			"users": users,
// 		})
// 	})

// 	srv.Run()
// }

// func doWork(ctx context.Context, i int) {
// 	sp, spCtx := ksmux.StartSpan(ctx, "doWork-"+strconv.Itoa(i))
// 	defer sp.End()
// 	sp.SetTag("bla", "value1")
// 	if i == 1 {
// 		doSubWork(spCtx, i)
// 	}
// 	time.Sleep(time.Duration(rand.IntN(2)) * time.Second)
// }

// func doSubWork(ctx context.Context, i int) {
// 	sp, _ := ksmux.StartSpan(ctx, "doSubWork-"+strconv.Itoa(i))
// 	defer sp.End()
// 	sp.SetTag("bla", "value1")
// 	time.Sleep(time.Duration(rand.IntN(2)) * time.Second)
// }
