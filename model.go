package cassandra

import (
	"context"
	"time"
)

type Proxy interface {
	BeginTransaction(ctx context.Context, timeout int64) (string, error)
	CommitTransaction(ctx context.Context, tx string) error
	RollbackTransaction(ctx context.Context, tx string) error
	Exec(ctx context.Context, query string, values ...interface{}) (int64, error)
	ExecBatch(ctx context.Context, master bool, stm ...Statement) (int64, error)
	Query(ctx context.Context, result interface{}, query string, values ...interface{}) error
	ExecWithTx(ctx context.Context, tx string, commit bool, query string, values ...interface{}) (int64, error)
	ExecBatchWithTx(ctx context.Context, tx string, commit bool, master bool, stm ...Statement) (int64, error)
	QueryWithTx(ctx context.Context, tx string, commit bool, result interface{}, query string, values ...interface{}) error
}

type JStatement struct {
	Query  string        `mapstructure:"query" json:"query,omitempty" gorm:"column:query" bson:"query,omitempty" dynamodbav:"query,omitempty" firestore:"query,omitempty"`
	Params []interface{} `mapstructure:"params" json:"params,omitempty" gorm:"column:params" bson:"params,omitempty" dynamodbav:"params,omitempty" firestore:"params,omitempty"`
	Dates  []int         `mapstructure:"dates" json:"dates,omitempty" gorm:"column:dates" bson:"dates,omitempty" dynamodbav:"dates,omitempty" firestore:"dates,omitempty"`
}
type Statement struct {
	Query  string        `mapstructure:"query" json:"query,omitempty" gorm:"column:query" bson:"query,omitempty" dynamodbav:"query,omitempty" firestore:"query,omitempty"`
	Params []interface{} `mapstructure:"params" json:"params,omitempty" gorm:"column:params" bson:"params,omitempty" dynamodbav:"params,omitempty" firestore:"params,omitempty"`
}
func BuildStatement(query string, values ...interface{}) *JStatement {
	stm := JStatement{Query: query}
	l := len(values)
	if l > 0 {
		ag2 := make([]interface{}, 0)
		dates := make([]int, 0)
		for i := 0; i < l; i++ {
			arg := values[i]
			if _, ok := arg.(time.Time); ok {
				dates = append(dates, i)
			} else if _, ok := arg.(*time.Time); ok {
				dates = append(dates, i)
			}
			ag2 = append(ag2, values[i])
		}
		stm.Params = ag2
		if len(dates) > 0 {
			stm.Dates = dates
		}
	}
	return &stm
}
func BuildJStatements(sts ...Statement) []JStatement {
	b := make([]JStatement, 0)
	if sts == nil || len(sts) == 0 {
		return b
	}
	for _, s := range sts {
		j := JStatement{Query: s.Query}
		if s.Params != nil && len(s.Params) > 0 {
			j.Params = s.Params
			j.Dates = ToDates(s.Params)
		}
		b = append(b, j)
	}
	return b
}
func ToDates(args []interface{}) []int {
	if args == nil || len(args) == 0 {
		ag2 := make([]int, 0)
		return ag2
	}
	var dates []int
	for i, arg := range args {
		if _, ok := arg.(time.Time); ok {
			dates = append(dates, i)
		}
		if _, ok := arg.(*time.Time); ok {
			dates = append(dates, i)
		}
	}
	return dates
}
