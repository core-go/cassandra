package batch

import (
	"context"
	"reflect"

	c "github.com/core-go/cassandra"
	"github.com/gocql/gocql"
)

type BatchUpdater struct {
	db           *gocql.ClusterConfig
	tableName    string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	VersionIndex int
	Schema       *c.Schema
}

func NewBatchUpdater(session *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewBatchUpdaterWithVersion(session, tableName, modelType, mp)
}
func NewBatchUpdaterWithVersion(session *gocql.ClusterConfig, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), options ...int) *BatchUpdater {
	versionIndex := -1
	if len(options) > 0 && options[0] >= 0 {
		versionIndex = options[0]
	}
	schema := c.CreateSchema(modelType)
	return &BatchUpdater{db: session, tableName: tableName, Schema: schema, VersionIndex: versionIndex, Map: mp}
}
func (w *BatchUpdater) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)
	var models2 interface{}
	var er0 error
	if w.Map != nil {
		models2, er0 = c.MapModels(ctx, models, w.Map)
		if er0 != nil {
			s0 := reflect.ValueOf(models2)
			_, er0b := c.InterfaceSlice(models2)
			failIndices = c.ToArrayIndex(s0, failIndices)
			return successIndices, failIndices, er0b
		}
	} else {
		models2 = models
	}
	session, er0 := w.db.CreateSession()
	if er0 != nil {
		return successIndices, failIndices, er0
	}
	defer session.Close()
	_, err := c.UpdateBatchWithVersion(ctx, session, w.tableName, models2, w.VersionIndex, w.Schema)
	s := reflect.ValueOf(models)
	if err == nil {
		// Return full success
		successIndices = c.ToArrayIndex(s, successIndices)
		return successIndices, failIndices, err
	} else {
		// Return full fail
		failIndices = c.ToArrayIndex(s, failIndices)
	}
	return successIndices, failIndices, err
}