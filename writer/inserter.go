package writer

import (
	"context"
	"reflect"

	c "github.com/core-go/cassandra"
	"github.com/gocql/gocql"
)

type Inserter struct {
	db           *gocql.ClusterConfig
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	schema       *c.Schema
	VersionIndex int
}

func NewInserterWithMap(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), options ...int) *Inserter {
	versionIndex := -1
	if len(options) > 0 && options[0] >= 0 {
		versionIndex = options[0]
	}
	schema := c.CreateSchema(modelType)
	return &Inserter{db: db, tableName: tableName, Map: mp, schema: schema, VersionIndex: versionIndex}
}

func NewInserter(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...func(ctx context.Context, model interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewInserterWithMap(db, tableName, modelType, mp)
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
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
		_, err := c.InsertWithVersion(session, w.tableName, m2, w.VersionIndex, w.schema)
		return err
	}
	session, er0 := w.db.CreateSession()
	if er0 != nil {
		return er0
	}
	defer session.Close()
	_, err := c.InsertWithVersion(session, w.tableName, model, w.VersionIndex, w.schema)
	return err
}
