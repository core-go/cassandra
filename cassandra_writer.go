package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"
)

type CassandraWriter struct {
	session      *gocql.Session
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	schema       *Schema
	VersionIndex int
}

func NewCassandraWriter(session *gocql.Session, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *CassandraWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	schema := CreateSchema(modelType)
	return &CassandraWriter{session: session, tableName: tableName, Map: mp, schema: schema}
}
func (w *CassandraWriter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, err := Save(w.session, w.tableName, m2, w.schema)
		return err
	}
	_, err := Save(w.session, w.tableName, model, w.schema)
	return err
}
