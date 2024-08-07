package writer

import (
	"context"
	"reflect"

	"github.com/apache/cassandra-gocql-driver"
	c "github.com/core-go/cassandra"
)

type Updater[T any] struct {
	db           *gocql.ClusterConfig
	table        string
	Map          func(T)
	VersionIndex int
	schema       *c.Schema
}

func NewUpdater[T any](db *gocql.ClusterConfig, table string, options ...func(T)) *Updater[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewUpdaterWithVersion[T](db, table, mp)
}
func NewUpdaterWithVersion[T any](db *gocql.ClusterConfig, table string, mp func(T), options ...int) *Updater[T] {
	version := -1
	if len(options) > 0 && options[0] >= 0 {
		version = options[0]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := c.CreateSchema(modelType)
	return &Updater[T]{db: db, table: table, VersionIndex: version, schema: schema, Map: mp}
}

func (w *Updater[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	session, er0 := w.db.CreateSession()
	if er0 != nil {
		return er0
	}
	defer session.Close()
	return c.UpdateWithVersion(session, w.table, model, w.VersionIndex, w.schema)
}
