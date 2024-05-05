package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"
)

type StreamWriter struct {
	db           *gocql.ClusterConfig
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	schema       *Schema
	batchSize    int
	batch        []interface{}
}

func NewStreamWriter(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, batchSize int, options ...func(context.Context, interface{}) (interface{}, error)) *StreamWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	schema := CreateSchema(modelType)
	return &StreamWriter{db: db, schema: schema, tableName: tableName, batchSize: batchSize, Map: mp}
}

func (w *StreamWriter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		w.batch = append(w.batch, m2)
	} else {
		w.batch = append(w.batch, model)
	}
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamWriter) Flush(ctx context.Context) error {
	query, args, err := BuildToSaveBatch(w.tableName, w.batch, w.schema)
	if err != nil {
		return err
	}
	session, er0 := w.db.CreateSession()
	if er0 != nil {
		return er0
	}
	defer session.Close()
	_, err = Exec(session, query, args...)
	return err
}
