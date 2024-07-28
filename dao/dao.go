package dao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/cassandra-gocql-driver"
	"reflect"

	q "github.com/core-go/cassandra"
)

type Dao[T any, K any] struct {
	*Writer[*T]
	Map    map[string]int
	Fields string
	Keys   []string
	IdMap  bool
}

func NewDao[T any, K any](db *gocql.ClusterConfig, tableName string) (*Dao[T, K], error) {
	return NewDaoWithVersion[T, K](db, tableName, "")
}
func NewDaoWithVersion[T any, K any](db *gocql.ClusterConfig, tableName string, versionField string) (*Dao[T, K], error) {
	adapter, err := NewWriterWithVersion[*T](db, tableName, versionField)
	if err != nil {
		return nil, err
	}

	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		return nil, errors.New("T must be a struct")
	}

	_, primaryKeys := q.FindPrimaryKeys(modelType)
	var k K
	kType := reflect.TypeOf(k)
	idMap := false
	if len(primaryKeys) > 1 {
		if kType.Kind() == reflect.Map {
			idMap = true
		} else if kType.Kind() != reflect.Struct {
			return nil, errors.New("for composite keys, K must be a struct or a map")
		}
	}

	fieldsIndex, err := q.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	fields := q.BuildFieldsBySchema(adapter.Schema)
	return &Dao[T, K]{adapter, fieldsIndex, fields, primaryKeys, idMap}, nil
}
func (a *Dao[T, K]) All(ctx context.Context) ([]T, error) {
	var objs []T
	query := fmt.Sprintf("select %s from %s", a.Fields, a.Table)
	ses, err := a.DB.CreateSession()
	if err != nil {
		return objs, err
	}
	defer ses.Close()
	err = q.Query(ses, a.Map, &objs, query)
	return objs, err
}
func toMap(obj interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	im := make(map[string]interface{})
	er2 := json.Unmarshal(b, &im)
	return im, er2
}
func (a *Dao[T, K]) getId(k K) (interface{}, error) {
	if len(a.Keys) >= 2 && !a.IdMap {
		ri, err := toMap(k)
		return ri, err
	} else {
		return k, nil
	}
}
func (a *Dao[T, K]) Load(ctx context.Context, id K) (*T, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return nil, er0
	}
	var objs []T
	queryAll := fmt.Sprintf("select %s from %s ", a.Fields, a.Table)
	query, args := q.BuildFindById(queryAll, ip, a.JsonColumnMap, a.Schema.SKeys)
	ses, err := a.DB.CreateSession()
	if err != nil {
		return nil, err
	}
	defer ses.Close()
	err = q.Query(ses, a.Map, &objs, query, args...)
	if len(objs) > 0 {
		return &objs[0], nil
	}
	return nil, nil
}
func (a *Dao[T, K]) Exist(ctx context.Context, id K) (bool, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return false, er0
	}
	query := fmt.Sprintf("select %s from %s ", a.Schema.SColumns[0], a.Table)
	query1, args := q.BuildFindById(query, ip, a.JsonColumnMap, a.Schema.SKeys)
	ses, err := a.DB.CreateSession()
	if err != nil {
		return false, err
	}
	defer ses.Close()
	res, err := q.QueryMap(ses, nil, query1, args...)
	if err != nil {
		return false, err
	}
	if len(res) > 0 {
		return true, nil
	}
	return false, nil
}
func (a *Dao[T, K]) Delete(ctx context.Context, id K) (int64, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return -1, er0
	}
	query := fmt.Sprintf("delete from %s ", a.Table)
	query1, args := q.BuildFindById(query, ip, a.JsonColumnMap, a.Schema.SKeys)
	ses, err := a.DB.CreateSession()
	if err != nil {
		return 0, err
	}
	defer ses.Close()
	er2 := q.Exec(ses, query1, args...)
	if er2 == nil {
		return 1, er2
	}
	return 0, er2
}
