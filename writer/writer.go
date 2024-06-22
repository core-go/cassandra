package writer

import (
	"context"
	"reflect"

	c "github.com/core-go/cassandra"
	"github.com/gocql/gocql"
)

type Writer struct {
	db           *gocql.ClusterConfig
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	schema       *c.Schema
	VersionIndex int
}

func NewWriter(session *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Writer {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	schema := c.CreateSchema(modelType)
	return &Writer{db: session, tableName: tableName, Map: mp, schema: schema}
}
func (w *Writer) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		session, er0 := w.db.CreateSession()
		if er0 != nil {
			return er0
		}
		defer session.Close()
		_, err := c.Save(session, w.tableName, m2, w.schema)
		return err
	}
	session, er0 := w.db.CreateSession()
	if er0 != nil {
		return er0
	}
	defer session.Close()
	_, err := c.Save(session, w.tableName, model, w.schema)
	return err
}
