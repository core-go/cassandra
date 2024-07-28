package adapter

import (
	"context"
	"github.com/apache/cassandra-gocql-driver"
	"reflect"
	"strings"

	q "github.com/core-go/cassandra"
)

type Writer[T any] struct {
	DB             *gocql.ClusterConfig
	Table          string
	Schema         *q.Schema
	JsonColumnMap  map[string]string
	versionField   string
	versionIndex   int
	versionDBField string
}

func NewWriter[T any](db *gocql.ClusterConfig, tableName string) (*Writer[T], error) {
	return NewWriterWithVersion[T](db, tableName, "")
}
func NewWriterWithVersion[T any](db *gocql.ClusterConfig, tableName string, versionField string) (*Writer[T], error) {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := q.CreateSchema(modelType)
	jsonColumnMapT := q.MakeJsonColumnMap(modelType)
	jsonColumnMap := q.GetWritableColumns(schema.Fields, jsonColumnMapT)
	adapter := &Writer[T]{DB: db, Table: tableName, Schema: schema, JsonColumnMap: jsonColumnMap, versionField: "", versionIndex: -1}
	if len(versionField) > 0 {
		index := q.FindFieldIndex(modelType, versionField)
		if index >= 0 {
			_, dbFieldName, exist := q.GetFieldByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			adapter.versionField = versionField
			adapter.versionIndex = index
			adapter.versionDBField = dbFieldName
		}
	}
	return adapter, nil
}

func (a *Writer[T]) Create(ctx context.Context, model T) (int64, error) {
	query, args := q.BuildToInsertWithVersion(a.Table, model, a.versionIndex, false, a.Schema)
	ses, err := a.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := q.Exec(ses, query, args...)
	if er2 != nil {
		return 0, er2
	}
	return 1, nil
}
func (a *Writer[T]) Update(ctx context.Context, model T) (int64, error) {
	query, args := q.BuildToUpdateWithVersion(a.Table, model, a.versionIndex, a.Schema)
	ses, err := a.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := q.Exec(ses, query, args...)
	if er2 != nil {
		return 0, er2
	}
	return 1, nil
}
func (a *Writer[T]) Save(ctx context.Context, model T) (int64, error) {
	query, args := q.BuildToInsertWithVersion(a.Table, model, a.versionIndex, true, a.Schema)
	ses, err := a.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := q.Exec(ses, query, args...)
	if er2 != nil {
		return 0, er2
	}
	return 1, nil
}
func (a *Writer[T]) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	dbColumnMap := q.JSONToColumns(model, a.JsonColumnMap)
	query, values := q.BuildToPatchWithVersion(a.Table, dbColumnMap, a.Schema.SKeys, a.versionDBField)
	ses, err := a.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := q.Exec(ses, query, values...)
	if er2 == nil {
		return 1, er2
	}
	return 0, er2
}
