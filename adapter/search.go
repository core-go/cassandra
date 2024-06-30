package adapter

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"

	q "github.com/core-go/cassandra"
)

type SearchAdapter[T any, K any, F any] struct {
	*Adapter[T, K]
	BuildQuery func(F) (string, []interface{})
	Mp         func(*T)
	Map        map[string]int
}

func NewSearchAdapter[T any, K any, F any](db *gocql.ClusterConfig, table string, buildQuery func(F) (string, []interface{}), options ...func(*T)) (*SearchAdapter[T, K, F], error) {
	return NewSearchAdapterWithVersion[T, K, F](db, table, buildQuery, "", options...)
}
func NewSearchAdapterWithVersion[T any, K any, F any](db *gocql.ClusterConfig, table string, buildQuery func(F) (string, []interface{}), versionField string, opts ...func(*T)) (*SearchAdapter[T, K, F], error) {
	adapter, err := NewAdapterWithVersion[T, K](db, table, versionField)
	if err != nil {
		return nil, err
	}
	var mp func(*T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	fieldsIndex, err := q.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	builder := &SearchAdapter[T, K, F]{Adapter: adapter, Map: fieldsIndex, BuildQuery: buildQuery, Mp: mp}
	return builder, nil
}

func (b *SearchAdapter[T, K, F]) Search(ctx context.Context, filter F, limit int64, next string) ([]T, string, error) {
	var objs []T
	sql, params := b.BuildQuery(filter)
	ses, err := b.DB.CreateSession()
	defer ses.Close()

	if err != nil {
		return objs, "", err
	}
	nextPageToken, er2 := q.QueryWithMap(ses, b.Map, &objs, sql, params, limit, next)
	if b.Mp != nil {
		l := len(objs)
		for i := 0; i < l; i++ {
			b.Mp(&objs[i])
		}
	}
	return objs, nextPageToken, er2
}
