package export

import (
	"context"
	c "github.com/core-go/cassandra"
	"github.com/gocql/gocql"
	"reflect"
)

func NewExportRepository(db *gocql.ClusterConfig, modelType reflect.Type,
	buildQuery func(context.Context) (string, []interface{}),
	transform func(context.Context, interface{}) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter, error) {
	return NewExporter(db, modelType, buildQuery, transform, write, close)
}
func NewExportAdapter(db *gocql.ClusterConfig, modelType reflect.Type,
	buildQuery func(context.Context) (string, []interface{}),
	transform func(context.Context, interface{}) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter, error) {
	return NewExporter(db, modelType, buildQuery, transform, write, close)
}
func NewExportService(db *gocql.ClusterConfig, modelType reflect.Type,
	buildQuery func(context.Context) (string, []interface{}),
	transform func(context.Context, interface{}) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter, error) {
	return NewExporter(db, modelType, buildQuery, transform, write, close)
}

func NewExporter(db *gocql.ClusterConfig, modelType reflect.Type,
	buildQuery func(context.Context) (string, []interface{}),
	transform func(context.Context, interface{}) string,
	write func(p []byte) (n int, err error),
	close func() error,
) (*Exporter, error) {
	fieldsIndex, err := c.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	return &Exporter{DB: db, modelType: modelType, Write: write, Close: close, fieldsIndex: fieldsIndex, Transform: transform, BuildQuery: buildQuery}, nil
}

type Exporter struct {
	DB          *gocql.ClusterConfig
	modelType   reflect.Type
	fieldsIndex map[string]int
	Transform   func(context.Context, interface{}) string
	BuildQuery  func(context.Context) (string, []interface{})
	Write       func(p []byte) (n int, err error)
	Close       func() error
}

func (s *Exporter) Export(ctx context.Context) error {
	query, p := s.BuildQuery(ctx)
	session, err := s.DB.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()
	q := session.Query(query, p...)
	err = q.Exec()
	if err != nil {
		return err
	}
	return s.ScanAndWrite(ctx, q.Iter(), s.modelType)
}

func (s *Exporter) ScanAndWrite(ctx context.Context, iter *gocql.Iter, structType reflect.Type) error {
	defer s.Close()
	columns := c.GetColumns(iter.Columns())
	for {
		initModel := reflect.New(structType).Interface()
		r := c.StructScan(initModel, columns, s.fieldsIndex, -1)
		if !iter.Scan(r...) {
			return nil
		} else {
			err1 := s.TransformAndWrite(ctx, s.Write, initModel)
			if err1 != nil {
				return err1
			}
		}
	}
	return nil
}

func (s *Exporter) TransformAndWrite(ctx context.Context, write func(p []byte) (n int, err error), model interface{}) error {
	line := s.Transform(ctx, model)
	_, er := write([]byte(line))
	return er
}
