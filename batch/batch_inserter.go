package batch

import (
	"context"
	"reflect"

	"github.com/apache/cassandra-gocql-driver"
	c "github.com/core-go/cassandra"
)

type BatchInserter[T any] struct {
	db           *gocql.ClusterConfig
	table        string
	Map          func(*T)
	VersionIndex int
	Schema       *c.Schema
}

func NewBatchInserter[T any](db *gocql.ClusterConfig, table string, options ...func(*T)) *BatchInserter[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewBatchInserterWithVersion[T](db, table, mp)
}
func NewBatchInserterWithVersion[T any](db *gocql.ClusterConfig, table string, mp func(*T), options ...int) *BatchInserter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	versionIndex := -1
	if len(options) > 0 && options[0] >= 0 {
		versionIndex = options[0]
	}
	schema := c.CreateSchema(modelType)
	return &BatchInserter[T]{db: db, table: table, Schema: schema, VersionIndex: versionIndex, Map: mp}
}
func (w *BatchInserter[T]) Write(ctx context.Context, models []T) error {
	l := len(models)
	if l == 0 {
		return nil
	}
	if w.Map != nil {
		for i := 0; i < l; i++ {
			w.Map(&models[i])
		}
	}
	session, er0 := w.db.CreateSession()
	if er0 != nil {
		return er0
	}
	defer session.Close()
	_, err := c.InsertBatchWithSizeAndVersion(ctx, session, l, w.table, models, w.VersionIndex, w.Schema)
	return err
}
