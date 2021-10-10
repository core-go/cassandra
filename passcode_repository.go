package cassandra

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type PasscodeRepository struct {
	db            *gocql.ClusterConfig
	tableName     string
	idName        string
	passcodeName  string
	expiredAtName string
}
func NewPasscodeAdapter(db *gocql.ClusterConfig, tableName string, options ...string) *PasscodeRepository {
	return NewPasscodeRepository(db, tableName, options...)
}
func NewPasscodeRepository(db *gocql.ClusterConfig, tableName string, options ...string) *PasscodeRepository {
	var idName, passcodeName, expiredAtName string
	if len(options) >= 1 && len(options[0]) > 0 {
		expiredAtName = options[0]
	} else {
		expiredAtName = "expiredat"
	}
	if len(options) >= 2 && len(options[1]) > 0 {
		idName = options[1]
	} else {
		idName = "id"
	}
	if len(options) >= 3 && len(options[2]) > 0 {
		passcodeName = options[2]
	} else {
		passcodeName = "passcode"
	}
	return &PasscodeRepository{
		db:            db,
		tableName:     strings.ToLower(tableName),
		idName:        strings.ToLower(idName),
		passcodeName:  strings.ToLower(passcodeName),
		expiredAtName: strings.ToLower(expiredAtName),
	}
}

func (p *PasscodeRepository) Save(ctx context.Context, id string, passcode string, expiredAt time.Time) (int64, error) {
	session, er0 := p.db.CreateSession()
	columns := []string{p.idName, p.passcodeName, p.expiredAtName}
	if er0 != nil {
		return 0, er0
	}
	queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES (? ,? ,?)",
		p.tableName,
		strings.Join(columns, ","),
	)

	err := session.Query(queryString, id, passcode, expiredAt).Exec()
	if err != nil {
		return 0, err
	}
	defer session.Close()
	return 1, nil
}

func (p *PasscodeRepository) Load(ctx context.Context, id string) (string, time.Time, error) {
	session, er0 := p.db.CreateSession()
	// var returnId strng
	var code string
	var expiredAt time.Time
	if er0 != nil {
		return "", time.Now().Add(-24 * time.Hour), er0
	}
	strSql := fmt.Sprintf(`SELECT %s, %s FROM `, p.passcodeName, p.expiredAtName) + p.tableName + ` WHERE ` + p.idName + ` =? ALLOW FILTERING`
	er1 := session.Query(strSql, id).Scan(&code, &expiredAt)
	if er1 != nil {
		return "", time.Now().Add(-24 * time.Hour), er1
	}
	return code, expiredAt, nil
}

func (p *PasscodeRepository) Delete(ctx context.Context, id string) (int64, error) {
	session, er0 := p.db.CreateSession()
	if er0 != nil {
		return 0, er0
	}
	query := "delete from " + p.tableName + " where " + p.idName + " = ?"
	er1 := session.Query(query, id).Exec()
	if er1 != nil {
		return 0, er1
	}
	return 1, nil
}
