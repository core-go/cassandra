package writer

import (
	"context"
	"reflect"

	"github.com/apache/cassandra-gocql-driver"
	c "github.com/core-go/cassandra"
)

type Inserter[T any] struct {
	db           *gocql.ClusterConfig
	table        string
	Map          func(T)
	schema       *c.Schema
	VersionIndex int
}

func NewInserterWithMap[T any](db *gocql.ClusterConfig, table string, mp func(T), options ...int) *Inserter[T] {
	versionIndex := -1
	if len(options) > 0 && options[0] >= 0 {
		versionIndex = options[0]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := c.CreateSchema(modelType)
	return &Inserter[T]{db: db, table: table, Map: mp, schema: schema, VersionIndex: versionIndex}
}

func NewInserter[T any](db *gocql.ClusterConfig, table string, options ...func(T)) *Inserter[T] {
	var mp func(T)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewInserterWithMap[T](db, table, mp)
}

func (w *Inserter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	session, er0 := w.db.CreateSession()
	if er0 != nil {
		return er0
	}
	defer session.Close()
	return c.InsertWithVersion(session, w.table, model, w.VersionIndex, w.schema)
}
