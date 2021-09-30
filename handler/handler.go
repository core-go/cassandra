package handler

import (
	"context"
	"encoding/json"
	c "github.com/core-go/cassandra"
	"github.com/gocql/gocql"
	"net/http"
)

type Handler struct {
	Session *gocql.Session
	Error   func(context.Context, string)
}

func NewHandler(session *gocql.Session, options ...func(context.Context, string)) *Handler {
	var logError func(context.Context, string)
	if len(options) >= 1 {
		logError = options[0]
	}
	return &Handler{Session: session, Error: logError}
}

func (h *Handler) Exec(w http.ResponseWriter, r *http.Request) {
	s := c.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	s.Params = c.ParseDates(s.Params, s.Dates)
	res, er1 := c.Exec(h.Session, s.Query, s.Params...)
	if er1 != nil {
		handleError(w, r, http.StatusInternalServerError, er1.Error(), h.Error, er1)
		return
	}
	succeed(w, r, http.StatusOK, res)
}

func (h *Handler) Query(w http.ResponseWriter, r *http.Request) {
	s := c.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	s.Params = c.ParseDates(s.Params, s.Dates)
	res, err := c.QueryMap(h.Session, s.Query, s.Params...)
	if err != nil {
		handleError(w, r, 500, err.Error(), h.Error, err)
		return
	}
	succeed(w, r, http.StatusOK, res)
}

func (h *Handler) ExecBatch(w http.ResponseWriter, r *http.Request) {
	var s []c.JStatement
	b := make([]c.Statement, 0)
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	l := len(s)
	for i := 0; i < l; i++ {
		st := c.Statement{}
		st.Query = s[i].Query
		st.Params = c.ParseDates(s[i].Params, s[i].Dates)
		b = append(b, st)
	}
	res, err := c.ExecuteAll(r.Context(), h.Session, b...)
	if err != nil {
		handleError(w, r, 500, err.Error(), h.Error, err)
		return
	}
	succeed(w, r, http.StatusOK, res)
}

func handleError(w http.ResponseWriter, r *http.Request, code int, result interface{}, logError func(context.Context, string), err error) {
	if logError != nil {
		logError(r.Context(), err.Error())
	}
	returnJSON(w, code, result)
}
func succeed(w http.ResponseWriter, r *http.Request, code int, result interface{}) {
	response, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}
func returnJSON(w http.ResponseWriter, code int, result interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if result == nil {
		w.Write([]byte("null"))
		return nil
	}
	response, err := marshal(result)
	if err != nil {
		// log.Println("cannot marshal of result: " + err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	w.Write(response)
	return nil
}
func marshal(v interface{}) ([]byte, error) {
	b, ok1 := v.([]byte)
	if ok1 {
		return b, nil
	}
	s, ok2 := v.(string)
	if ok2 {
		return []byte(s), nil
	}
	return json.Marshal(v)
}
