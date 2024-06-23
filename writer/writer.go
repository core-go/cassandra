package writer

import (
	"context"
	"reflect"

	c "github.com/core-go/cassandra"
	"github.com/gocql/gocql"
)

type Writer[T any] struct {
	db           *gocql.ClusterConfig
	table        string
	Map          func(T)
	schema       *c.Schema
	VersionIndex int
}

func NewWriter[T any](session *gocql.ClusterConfig, table string, modelType reflect.Type, options ...func(T)) *Writer[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	schema := c.CreateSchema(modelType)
	return &Writer[T]{db: session, table: table, Map: mp, schema: schema}
}
func (w *Writer[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	session, er0 := w.db.CreateSession()
	if er0 != nil {
		return er0
	}
	defer session.Close()
	return c.Save(session, w.table, model, w.schema)
}
