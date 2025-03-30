package korm

import (
	"net/http"

	"github.com/kamalshkeir/aes"
	"github.com/kamalshkeir/argon"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
)

var LoginView = func(c *ksmux.Context) {
	c.Html("admin/admin_login.html", nil)
}

var LoginPOSTView = func(c *ksmux.Context) {
	requestData := c.BodyJson()
	email := requestData["email"]
	passRequest := requestData["password"]

	data, err := Table("users").Database(defaultDB).Where("email = ?", email).One()
	if err != nil {
		c.Status(http.StatusUnauthorized).Json(map[string]any{
			"error": err.Error(),
		})
		return
	}
	if data["email"] == "" || data["email"] == nil {
		c.Status(http.StatusNotFound).Json(map[string]any{
			"error": "User doesn not Exist",
		})
		return
	}
	if data["is_admin"] == int64(0) || data["is_admin"] == 0 || data["is_admin"] == false {
		c.Status(http.StatusForbidden).Json(map[string]any{
			"error": "Not Allowed to access this page",
		})
		return
	}

	if passDB, ok := data["password"].(string); ok {
		if pp, ok := passRequest.(string); ok {
			if !argon.Match(passDB, pp) {
				c.Status(http.StatusForbidden).Json(map[string]any{
					"error": "Wrong Password",
				})
				return
			} else {
				if uuid, ok := data["uuid"].(string); ok {
					uuid, err = aes.Encrypt(uuid)
					lg.CheckError(err)
					c.SetCookie("session", uuid)
					c.Json(map[string]any{
						"success": "U Are Logged In",
					})
					return
				}
			}
		}
	}
}

var LogoutView = func(c *ksmux.Context) {
	c.DeleteCookie("session")
	c.Status(http.StatusTemporaryRedirect).Redirect("/")
}
