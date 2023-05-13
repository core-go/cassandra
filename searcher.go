package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"
)

type Searcher struct {
	search  func(ctx context.Context, searchModel interface{}, results interface{}, limit int64, options ...int64) (int64, string, error)
}
func NewSearcher(search func(context.Context, interface{}, interface{}, int64, ...int64) (int64, string, error)) *Searcher {
	return &Searcher{search: search}
}

func (s *Searcher) Search(ctx context.Context, m interface{}, results interface{}, limit int64, options ...int64) (int64, string, error) {
	return s.search(ctx, m, results, limit, options...)
}
func NewSearcherWithQuery(db *gocql.ClusterConfig, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), pageState string, options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, error) {
	builder, err := NewSearchBuilder(db, modelType, buildQuery, pageState, options...)
	if err != nil {
		return nil, err
	}
	return NewSearcher(builder.Search), nil
}