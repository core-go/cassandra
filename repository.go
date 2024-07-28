package cassandra

import (
	"context"
	"github.com/apache/cassandra-gocql-driver"
	"reflect"
)

func NewViewRepository(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) (*Loader, error) {
	return NewLoader(db, tableName, modelType, options...)
}

func NewRepository(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...Mapper) (*Writer, error) {
	return NewWriter(db, tableName, modelType, options...)
}
func NewRepositoryWithVersion(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, versionField string, options ...Mapper) (*Writer, error) {
	return NewWriterWithVersion(db, tableName, modelType, versionField, options...)
}
func NewViewAdapter(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) (*Loader, error) {
	return NewLoader(db, tableName, modelType, options...)
}

func NewAdapter(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...Mapper) (*Writer, error) {
	return NewWriter(db, tableName, modelType, options...)
}
func NewAdapterWithVersion(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, versionField string, options ...Mapper) (*Writer, error) {
	return NewWriterWithVersion(db, tableName, modelType, versionField, options...)
}
