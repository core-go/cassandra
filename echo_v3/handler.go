package echo

import (
	"context"
	"encoding/json"
	c "github.com/core-go/cassandra"
	"github.com/gocql/gocql"
	"github.com/labstack/echo"
	"net/http"
)

type Handler struct {
	DB        *gocql.ClusterConfig
	Transform func(s string) string
	Error     func(context.Context, string)
}

func NewHandler(db *gocql.ClusterConfig, transform func(s string) string, options ...func(context.Context, string)) *Handler {
	var logError func(context.Context, string)
	if len(options) >= 1 {
		logError = options[0]
	}
	return &Handler{DB: db, Transform: transform, Error: logError}
}

func (h *Handler) Exec(ctx echo.Context) error {
	r := ctx.Request()
	s := c.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return er0
	}
	s.Params = c.ParseDates(s.Params, s.Dates)
	session, err := h.DB.CreateSession()
	if err != nil {
		handleError(ctx, http.StatusInternalServerError, err.Error(), h.Error, err)
		return err
	}
	defer session.Close()
	res, er1 := c.Exec(session, s.Query, s.Params...)
	if er1 != nil {
		handleError(ctx, http.StatusInternalServerError, er1.Error(), h.Error, er1)
		return er1
	}
	return ctx.JSON(http.StatusOK, res)
}

func (h *Handler) Query(ctx echo.Context) error {
	r := ctx.Request()
	s := c.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return er0
	}
	s.Params = c.ParseDates(s.Params, s.Dates)
	session, err := h.DB.CreateSession()
	if err != nil {
		handleError(ctx, http.StatusInternalServerError, err.Error(), h.Error, err)
		return err
	}
	defer session.Close()
	res, er1 := c.QueryMap(session, h.Transform, s.Query, s.Params...)
	if er1 != nil {
		handleError(ctx, http.StatusInternalServerError, er1.Error(), h.Error, er1)
		return er1
	}
	return ctx.JSON(http.StatusOK, res)
}

func (h *Handler) ExecBatch(ctx echo.Context) error {
	r := ctx.Request()
	var s []c.JStatement
	b := make([]c.Statement, 0)
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return er0
	}
	l := len(s)
	for i := 0; i < l; i++ {
		st := c.Statement{}
		st.Query = s[i].Query
		st.Params = c.ParseDates(s[i].Params, s[i].Dates)
		b = append(b, st)
	}
	session, err := h.DB.CreateSession()
	if err != nil {
		handleError(ctx, http.StatusInternalServerError, err.Error(), h.Error, err)
		return err
	}
	defer session.Close()
	res, er1 := c.ExecuteAll(r.Context(), session, b...)
	if er1 != nil {
		handleError(ctx, http.StatusInternalServerError, er1.Error(), h.Error, er1)
		return er1
	}
	return ctx.JSON(http.StatusOK, res)
}

func handleError(ctx echo.Context, code int, result interface{}, logError func(context.Context, string), err error) {
	if logError != nil {
		logError(ctx.Request().Context(), err.Error())
	}
	ctx.JSON(code, result)
}
