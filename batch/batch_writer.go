package batch

import (
	"context"
	"reflect"

	c "github.com/core-go/cassandra"
	"github.com/gocql/gocql"
)

type BatchWriter[T any] struct {
	db           *gocql.ClusterConfig
	table        string
	Map          func(*T)
	VersionIndex int
	Schema       *c.Schema
}

func NewBatchWriter[T any](session *gocql.ClusterConfig, table string, options ...func(*T)) *BatchWriter[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewBatchWriterWithVersion[T](session, table, mp)
}
func NewBatchWriterWithVersion[T any](session *gocql.ClusterConfig, table string, mp func(*T), options ...int) *BatchWriter[T] {
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
	return &BatchWriter[T]{db: session, table: table, Schema: schema, VersionIndex: versionIndex, Map: mp}
}
func (w *BatchWriter[T]) Write(ctx context.Context, models []T) error {
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
	_, err := c.SaveBatchWithSize(ctx, session, l, w.table, models, w.Schema)
	return err
}
