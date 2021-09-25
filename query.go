package cassandra

import (
	"encoding/hex"
	"github.com/gocql/gocql"
)

func QueryMap(ses *gocql.Session, sql string, values ...interface{}) ([]map[string]interface{}, error) {
	q := ses.Query(sql, values...)
	list := make([]map[string]interface{}, 0)
	if q.Exec() != nil {
		return list, q.Exec()
	}
	iter := q.Iter()
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			return list, nil
		} else {
			list = append(list, row)
		}
	}
}
func Query(ses *gocql.Session, results interface{}, sql string, values ...interface{}) error {
	q := ses.Query(sql, values...)
	if q.Exec() != nil {
		return q.Exec()
	}
	return ScanIter(q.Iter(), results)
}
func QueryWithPage(ses *gocql.Session, max int, nextPageToken string, results interface{}, sql string, values ...interface{}) (string, error) {
	if len(nextPageToken) == 0 {
		query := ses.Query(sql, values...).PageSize(max)
		if query.Exec() != nil {
			return "", query.Exec()
		}
		err := ScanIter(query.Iter(), results)
		if err != nil {
			return "", err
		}
		nextPageToken := hex.EncodeToString(query.Iter().PageState())
		return nextPageToken, nil
	} else {
		next, er0 := hex.DecodeString(nextPageToken)
		if er0 != nil {
			return "", er0
		}
		query := ses.Query(sql, values...).PageState(next).PageSize(max)
		if query.Exec() != nil {
			return "", query.Exec()
		}
		err := ScanIter(query.Iter(), results)
		if err != nil {
			return "", err
		}
		nextPageToken := hex.EncodeToString(query.Iter().PageState())
		return nextPageToken, nil
	}
}
