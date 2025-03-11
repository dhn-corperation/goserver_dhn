package kaocommon

import(
	"fmt"
	"context"
	s "strings"
	"database/sql"

	config "mycs/src/kaoconfig"
	databasepool "mycs/src/kaodatabasepool"
)

//물음표 컬럼 개수만큼 조인
func GetQuestionMark(column []string) string {
	var placeholders []string
	numPlaceholders := len(column) // 원하는 물음표 수
	for i := 0; i < numPlaceholders; i++ {
	    placeholders = append(placeholders, "?")
	}
	return s.Join(placeholders, ",")
}

//테이블 insert 처리
func InsMsg(query string, insStrs []string, insValues []interface{}) ([]string, []interface{}){
	var errlog = config.Stdlog
	stmt := fmt.Sprintf(query, s.Join(insStrs, ","))

	tx, err := databasepool.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})

	if err != nil{
		config.Stdlog.Println("InsMsg init tx : ",err)
		return InsMsg(query, insStrs, insValues)
	}

	_, err = tx.Exec(stmt, insValues...)

	if err != nil {
		errlog.Println("Result Table Insert 처리 중 오류 발생 ", err.Error())
		errlog.Println("table : ", query)
		return InsMsg(query, insStrs, insValues)
	}

	if err := tx.Commit(); err != nil {
		config.Stdlog.Println("Result Table Insert tx Commit 오류 발생 : ", err)
		return InsMsg(query, insStrs, insValues)
	}
	return nil, nil
}