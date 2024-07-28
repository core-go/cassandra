package repository

import (
	"context"
	"github.com/apache/cassandra-gocql-driver"
	"reflect"

	q "github.com/core-go/cassandra"
)

type SearchRepository[T any, K any, F any] struct {
	*Repository[T, K]
	BuildQuery func(F) (string, []interface{})
	Mp         func(*T)
	Map        map[string]int
}

func NewSearchRepository[T any, K any, F any](db *gocql.ClusterConfig, table string, buildQuery func(F) (string, []interface{}), options ...func(*T)) (*SearchRepository[T, K, F], error) {
	return NewSearchRepositoryWithVersion[T, K, F](db, table, buildQuery, "", options...)
}
func NewSearchRepositoryWithVersion[T any, K any, F any](db *gocql.ClusterConfig, table string, buildQuery func(F) (string, []interface{}), versionField string, opts ...func(*T)) (*SearchRepository[T, K, F], error) {
	repo, err := NewRepositoryWithVersion[T, K](db, table, versionField)
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
	builder := &SearchRepository[T, K, F]{Repository: repo, Map: fieldsIndex, BuildQuery: buildQuery, Mp: mp}
	return builder, nil
}

func (b *SearchRepository[T, K, F]) Search(ctx context.Context, filter F, limit int64, next string) ([]T, string, error) {
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
