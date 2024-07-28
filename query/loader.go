package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/cassandra-gocql-driver"
	"reflect"
	"strings"

	q "github.com/core-go/cassandra"
)

type Loader[T any, K any] struct {
	DB            *gocql.ClusterConfig
	Table         string
	Map           map[string]int
	JsonColumnMap map[string]string
	Fields        string
	Keys          []string
	IdMap         bool
	field1        string
}

func NewLoader[T any, K any](db *gocql.ClusterConfig, tableName string) (*Loader[T, K], error) {
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
	fields := q.GetFields(modelType)
	if len(fields) == 0 {
		return nil, fmt.Errorf("require at least 1 field of table %s", tableName)
	}
	field1 := fields[0]

	jsonColumnKeys := q.MapJsonColumn(modelType)
	fieldsIndex, err := q.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	return &Loader[T, K]{db, tableName, fieldsIndex, jsonColumnKeys, strings.Join(fields, ","), primaryKeys, idMap, field1}, nil
}
func (a *Loader[T, K]) All(ctx context.Context) ([]T, error) {
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
func (a *Loader[T, K]) getId(k K) (interface{}, error) {
	if len(a.Keys) >= 2 && !a.IdMap {
		ri, err := toMap(k)
		return ri, err
	} else {
		return k, nil
	}
}
func (a *Loader[T, K]) Load(ctx context.Context, id K) (*T, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return nil, er0
	}
	var objs []T
	queryAll := fmt.Sprintf("select %s from %s ", a.Fields, a.Table)
	query, args := q.BuildFindById(queryAll, ip, a.JsonColumnMap, a.Keys)
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
func (a *Loader[T, K]) Exist(ctx context.Context, id K) (bool, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return false, er0
	}
	query := fmt.Sprintf("select %s from %s ", a.field1, a.Table)
	query1, args := q.BuildFindById(query, ip, a.JsonColumnMap, a.Keys)
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
