package export

import (
	"context"
	"github.com/gocql/gocql"
	"reflect"
)

func NewExportAdapter[T any](db *gocql.ClusterConfig,
	buildQuery func(context.Context) (string, []interface{}),
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter[T], error) {
	return NewExporter[T](db, buildQuery, transform, write, close)
}
func NewExportService[T any](db *gocql.ClusterConfig,
	buildQuery func(context.Context) (string, []interface{}),
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter[T], error) {
	return NewExporter[T](db, buildQuery, transform, write, close)
}

func NewExporter[T any](db *gocql.ClusterConfig,
	buildQuery func(context.Context) (string, []interface{}),
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter[T], error) {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	fieldsIndex, err := GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	return &Exporter[T]{DB: db, Write: write, Close: close, Map: fieldsIndex, Transform: transform, BuildQuery: buildQuery}, nil
}

type Exporter[T any] struct {
	DB         *gocql.ClusterConfig
	Map        map[string]int
	Transform  func(context.Context, *T) string
	BuildQuery func(context.Context) (string, []interface{})
	Write      func(p []byte) (n int, err error)
	Close      func() error
}

func (s *Exporter[T]) Export(ctx context.Context) (int64, error) {
	query, p := s.BuildQuery(ctx)
	session, err := s.DB.CreateSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()
	q := session.Query(query, p...)
	err = q.Exec()
	if err != nil {
		return 0, err
	}
	return s.ScanAndWrite(ctx, q.Iter())
}

func (s *Exporter[T]) ScanAndWrite(ctx context.Context, iter *gocql.Iter) (int64, error) {
	defer s.Close()
	columns := GetColumns(iter.Columns())
	var i int64
	i = 0
	for {
		var obj T
		r := StructScan(&obj, columns, s.Map, -1)
		if !iter.Scan(r...) {
			return i, nil
		} else {
			er1 := s.TransformAndWrite(ctx, s.Write, &obj)
			if er1 != nil {
				return i, er1
			}
		}
	}
}

func (s *Exporter[T]) TransformAndWrite(ctx context.Context, write func(p []byte) (n int, err error), model *T) error {
	line := s.Transform(ctx, model)
	_, er := write([]byte(line))
	return er
}
