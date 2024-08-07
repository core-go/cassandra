package cassandra

import (
	"github.com/apache/cassandra-gocql-driver"
	"reflect"
)

func NewSearchWriterWithVersionAndMap(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), versionField string, mapper Mapper) (*Searcher, *Writer, error) {
	if mapper == nil {
		searcher, er0 := NewSearcherWithQuery(db, modelType, buildQuery)
		if er0 != nil {
			return searcher, nil, er0
		}
		writer, er1 := NewWriterWithVersion(db, tableName, modelType, versionField, mapper)
		return searcher, writer, er1
	} else {
		searcher, er0 := NewSearcherWithQuery(db, modelType, buildQuery, mapper.DbToModel)
		if er0 != nil {
			return searcher, nil, er0
		}
		writer, er1 := NewWriterWithVersion(db, tableName, modelType, versionField, mapper)
		return searcher, writer, er1
	}
}
func NewSearchWriterWithVersion(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), versionField string, options ...Mapper) (*Searcher, *Writer, error) {
	var mapper Mapper
	if len(options) > 0 {
		mapper = options[0]
	}
	return NewSearchWriterWithVersionAndMap(db, tableName, modelType, buildQuery, versionField, mapper)
}
func NewSearchWriterWithMap(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), mapper Mapper, options ...string) (*Searcher, *Writer, error) {
	var versionField string
	if len(options) > 0 {
		versionField = options[0]
	}
	return NewSearchWriterWithVersionAndMap(db, tableName, modelType, buildQuery, versionField, mapper)
}
func NewSearchWriter(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), pageState string, options ...Mapper) (*Searcher, *Writer, error) {
	var mapper Mapper
	if len(options) > 0 {
		mapper = options[0]
	}
	return NewSearchWriterWithVersionAndMap(db, tableName, modelType, buildQuery, "", mapper)
}
