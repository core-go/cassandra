package cassandra

import (
	"context"
	"github.com/apache/cassandra-gocql-driver"
	"reflect"
)

func NewSearchLoader(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, *Loader, error) {
	return NewSqlSearchLoader(db, tableName, modelType, buildQuery, options...)
}

func NewSqlSearchLoader(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, *Loader, error) {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	loader, er0 := NewLoader(db, tableName, modelType, mp)
	if er0 != nil {
		return nil, loader, er0
	}
	searcher, er1 := NewSearcherWithQuery(db, modelType, buildQuery, options...)
	return searcher, loader, er1
}
