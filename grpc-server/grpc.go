package grpc_server

import (
	"bytes"
	"context"
	"encoding/json"
	c "github.com/core-go/cassandra"
	"github.com/core-go/cassandra/grpc"
	"github.com/gocql/gocql"
)

type GRPCHandler struct {
	grpc.DbProxyServer
	Session *gocql.Session
	Error   func(context.Context, string)
}

func NewHandler(db *gocql.Session, err func(context.Context, string)) *GRPCHandler {
	g := GRPCHandler{
		Session: db,
		Error:   err,
	}
	return &g
}

func CreateStatements(in *grpc.BatchRequest) ([]c.JStatement, error) {
	var (
		statements []c.JStatement
		err        error
	)
	for _, batch := range in.Batch {
		st := c.JStatement{
			Query: batch.Query,
		}
		err = json.NewDecoder(bytes.NewBuffer(batch.Params)).Decode(&st.Params)
		if err != nil {
			return nil, err
		}
		for _, date := range batch.Dates {
			st.Dates = append(st.Dates, int(date))
		}
		statements = append(statements, st)
	}
	return statements, err
}

func (s *GRPCHandler) Query(ctx context.Context, in *grpc.Request) (*grpc.QueryResponse, error) {
	statement := c.JStatement{}
	err := json.NewDecoder(bytes.NewBuffer(in.Params)).Decode(&statement.Params)
	if err != nil {
		return &grpc.QueryResponse{Message: "Error: " + err.Error()}, err
	}
	statement.Query = in.Query
	for _, v := range in.Dates {
		statement.Dates = append(statement.Dates, int(v))
	}
	statement.Params = c.ParseDates(statement.Params, statement.Dates)
	res, err := c.QueryMap(s.Session, statement.Query, statement.Params...)
	data := new(bytes.Buffer)
	err = json.NewEncoder(data).Encode(&res)
	if err != nil {
		return &grpc.QueryResponse{Message: "Error: " + err.Error()}, err
	}
	return &grpc.QueryResponse{
		Message: data.String(),
	}, err
}

func (s *GRPCHandler) Execute(ctx context.Context, in *grpc.Request) (*grpc.Response, error) {
	statement := c.JStatement{}
	er0 := json.NewDecoder(bytes.NewBuffer(in.Params)).Decode(&statement.Params)
	if er0 != nil {
		return &grpc.Response{Result: -1}, er0
	}
	statement.Query = in.Query
	for _, v := range in.Dates {
		statement.Dates = append(statement.Dates, int(v))
	}
	statement.Params = c.ParseDates(statement.Params, statement.Dates)
	result, er1 := c.Exec(s.Session, statement.Query, statement.Params...)
	if er1 != nil {
		return &grpc.Response{Result: -1}, er1
	}
	return &grpc.Response{Result: result}, er1
}

func (s *GRPCHandler) ExecBatch(ctx context.Context, in *grpc.BatchRequest) (*grpc.Response, error) {
	statements, err := CreateStatements(in)
	if err != nil {
		return &grpc.Response{Result: -1}, err
	}
	b := make([]c.Statement, 0)
	l := len(statements)
	for i := 0; i < l; i++ {
		st := c.Statement{}
		st.Query = statements[i].Query
		st.Params = c.ParseDates(statements[i].Params, statements[i].Dates)
		b = append(b, st)
	}
	res, err := c.ExecuteAll(ctx, s.Session, b...)
	return &grpc.Response{Result: res}, err
}
