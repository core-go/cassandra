package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"
)

type Searcher struct {
	search  func(ctx context.Context, searchModel interface{}, results interface{}, limit int64, nextPageToken string) (string, error)
}
func NewSearcher(search func(context.Context, interface{}, interface{}, int64, string) (string, error)) *Searcher {
	return &Searcher{search: search}
}

func (s *Searcher) Search(ctx context.Context, m interface{}, results interface{}, limit int64, nextPageToken string) (string, error) {
	return s.search(ctx, m, results, limit, nextPageToken)
}
func NewSearcherWithQuery(db *gocql.ClusterConfig, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, error) {
	builder, err := NewSearchBuilder(db, modelType, buildQuery, options...)
	if err != nil {
		return nil, err
	}
	return NewSearcher(builder.Search), nil
}
