package korm

import (
	"context"
	"net/http"

	"github.com/kamalshkeir/aes"
	"github.com/kamalshkeir/ksmux"
)

var (
	BASIC_AUTH_USER = "notset"
	BASIC_AUTH_PASS = "testnotsetbutwaititshouldbeset"
)

var Auth = func(handler ksmux.Handler) ksmux.Handler {
	const key ksmux.ContextKey = "user"
	return func(c *ksmux.Context) {
		session, err := c.GetCookie("session")
		if err != nil || session == "" {
			// NOT AUTHENTICATED
			c.DeleteCookie("session")
			handler(c)
			return
		}
		session, err = aes.Decrypt(session)
		if err != nil {
			handler(c)
			return
		}
		// Check session
		user, err := Model[User]().Where("uuid = ?", session).One()
		if err != nil {
			// session fail
			handler(c)
			return
		}

		// AUTHENTICATED AND FOUND IN DB
		ctx := context.WithValue(c.Request.Context(), key, user)
		*c = ksmux.Context{
			Params:         c.Params,
			Request:        c.Request.WithContext(ctx),
			ResponseWriter: c.ResponseWriter,
		}
		handler(c)
	}
}

var Admin = func(handler ksmux.Handler) ksmux.Handler {
	const key ksmux.ContextKey = "user"
	return func(c *ksmux.Context) {
		session, err := c.GetCookie("session")
		if err != nil || session == "" {
			// NOT AUTHENTICATED
			c.DeleteCookie("session")
			c.Status(http.StatusTemporaryRedirect).Redirect(adminPathNameGroup + "/login")
			return
		}
		session, err = aes.Decrypt(session)
		if err != nil {
			c.Status(http.StatusTemporaryRedirect).Redirect(adminPathNameGroup + "/login")
			return
		}
		user, err := Model[User]().Where("uuid = ?", session).One()

		if err != nil {
			// AUTHENTICATED BUT NOT FOUND IN DB
			c.Status(http.StatusTemporaryRedirect).Redirect(adminPathNameGroup + "/login")
			return
		}

		// Not admin
		if !user.IsAdmin {
			c.Status(403).Text("Middleware : Not allowed to access this page")
			return
		}

		ctx := context.WithValue(c.Request.Context(), key, user)
		*c = ksmux.Context{
			Params:         c.Params,
			Request:        c.Request.WithContext(ctx),
			ResponseWriter: c.ResponseWriter,
		}

		handler(c)
	}
}

var BasicAuth = func(handler ksmux.Handler) ksmux.Handler {
	return ksmux.BasicAuth(handler, BASIC_AUTH_USER, BASIC_AUTH_PASS)
}
