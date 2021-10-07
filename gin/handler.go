package echo

import (
	"context"
	"encoding/json"
	c "github.com/core-go/cassandra"
	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"net/http"
)

type Handler struct {
	Session   *gocql.Session
	Transform func(s string) string
	Error     func(context.Context, string)
}

func NewHandler(session *gocql.Session, transform func(s string) string, options ...func(context.Context, string)) *Handler {
	var logError func(context.Context, string)
	if len(options) >= 1 {
		logError = options[0]
	}
	return &Handler{Session: session, Transform: transform, Error: logError}
}

func (h *Handler) Exec(ctx *gin.Context) {
	r := ctx.Request
	s := c.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return
	}
	s.Params = c.ParseDates(s.Params, s.Dates)
	res, er1 := c.Exec(h.Session, s.Query, s.Params...)
	if er1 != nil {
		handleError(ctx, 500, er1.Error(), h.Error, er1)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

func (h *Handler) Query(ctx *gin.Context) {
	r := ctx.Request
	s := c.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return
	}
	s.Params = c.ParseDates(s.Params, s.Dates)
	res, er1 := c.QueryMap(h.Session, h.Transform, s.Query, s.Params...)
	if er1 != nil {
		handleError(ctx, http.StatusInternalServerError, er1.Error(), h.Error, er1)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

func (h *Handler) ExecBatch(ctx *gin.Context) {
	r := ctx.Request
	var s []c.JStatement
	b := make([]c.Statement, 0)
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return
	}
	l := len(s)
	for i := 0; i < l; i++ {
		st := c.Statement{}
		st.Query = s[i].Query
		st.Params = c.ParseDates(s[i].Params, s[i].Dates)
		b = append(b, st)
	}
	res, er1 := c.ExecuteAll(r.Context(), h.Session, b...)
	if er1 != nil {
		handleError(ctx, http.StatusInternalServerError, er1.Error(), h.Error, er1)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

func handleError(ctx *gin.Context, code int, result interface{}, logError func(context.Context, string), err error) {
	if logError != nil {
		logError(ctx.Request.Context(), err.Error())
	}
	ctx.JSON(code, result)
}
