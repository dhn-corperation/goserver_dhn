package kaosendrequest

import (
	//"encoding/json"
	config "mycs/src/kaoconfig"
	databasepool "mycs/src/kaodatabasepool"
	s "strings"
	"sync"
	"database/sql"
)

func ResultProc() {
	var wg sync.WaitGroup

	for {
		wg.Add(1)

		go resPollingProcess(&wg)

		wg.Wait()
	}
}

func resPollingProcess(wg *sync.WaitGroup) {

	defer wg.Done()
 
	var db = databasepool.DB
	//var conf = config.Conf
	//var stdlog = config.Stdlog
	var errlog = config.Stdlog

	pollingsql := "SELECT dpr.msg_id, dpr.type FROM DHN_POLLING_RESULT dpr INNER JOIN DHN_RESULT dr ON dpr.msg_id = dr.msgid WHERE dr.result = 'N' AND dr.sync = 'N'"

	resrows, err := db.Query(pollingsql)
	if err != nil {
		errlog.Fatal(err)
	}

	supmsgids := []interface{}{}
	fupmsgids := []interface{}{}
	
	for resrows.Next() {
		var msg_id sql.NullString
		var restype sql.NullString

		resrows.Scan(&msg_id, &restype)
		
		if s.EqualFold(restype.String, "S") {
			supmsgids = append(supmsgids, msg_id.String)
		} else {
			fupmsgids = append(fupmsgids, msg_id.String)
		}
		
	
		if len(supmsgids) >= 1000 {

			var commastr = "update DHN_RESULT set result = 'Y' where sync = 'N' and msgid in ("

			for i := 1; i < len(supmsgids); i++ {
				commastr = commastr + "?,"
			}

			commastr = commastr + "?)"

			_, err1 := db.Exec(commastr, supmsgids...)

			if err1 != nil {
				errlog.Println("Result Table S Update 처리 중 오류 발생 ")
			}
			
			commastr = "delete from DHN_POLLING_RESULT where msg_id in ("
			for i := 1; i < len(supmsgids); i++ {
				commastr = commastr + "?,"
			}
			commastr = commastr + "?)"
			_, err1 = db.Exec(commastr, supmsgids...)
			if err1 != nil {
				errlog.Println("Result Table S Delete 처리 중 오류 발생 ")
			}
			
			supmsgids = nil
		}

		if len(fupmsgids) >= 1000 {

			var commastr = "update DHN_RESULT set result = 'Y',code = '9999', message = 'ME09' where sync = 'N' and msgid in ("
			for i := 1; i < len(fupmsgids); i++ {
				commastr = commastr + "?,"
			}
			commastr = commastr + "?)"
			_, err1 := db.Exec(commastr, fupmsgids...)
			if err1 != nil {
				errlog.Println("Result Table F Update 처리 중 오류 발생 ")
			}

			commastr = "delete from DHN_POLLING_RESULT where msg_id in ("
			for i := 1; i < len(fupmsgids); i++ {
				commastr = commastr + "?,"
			}
			commastr = commastr + "?)"
			_, err1 = db.Exec(commastr, fupmsgids...)
			if err1 != nil {
				errlog.Println("Result Table F Delete 처리 중 오류 발생 ")
			}
			fupmsgids = nil
		}
	
	}

	if len(supmsgids) > 0 {

		var commastr = "update DHN_RESULT set result = 'Y' where sync = 'N' and msgid in ("
	
		for i := 1; i < len(supmsgids); i++ {
			commastr = commastr + "?,"
		}
	
		commastr = commastr + "?)"
	
		_, err1 := db.Exec(commastr, supmsgids...)
	
		if err1 != nil {
			errlog.Println("Result Table S Update 처리 중 오류 발생 ")
		}
		
		commastr = "delete from DHN_POLLING_RESULT where msg_id in ("
		for i := 1; i < len(supmsgids); i++ {
			commastr = commastr + "?,"
		}
		commastr = commastr + "?)"
		_, err1 = db.Exec(commastr, supmsgids...)
		if err1 != nil {
			errlog.Println("Result Table S Delete 처리 중 오류 발생 ")
		}
 
		supmsgids = nil
	}
	
	if len(fupmsgids) > 0 {
	
		var commastr = "update DHN_RESULT set result = 'Y',code = '9999', message = 'ME09' where sync = 'N' and msgid in ("
	
		for i := 1; i < len(fupmsgids); i++ {
			commastr = commastr + "?,"
		}
	
		commastr = commastr + "?)"
	
		_, err1 := db.Exec(commastr, fupmsgids...)
	
		if err1 != nil {
			errlog.Println("Result Table F Update 처리 중 오류 발생 ")
		}
	
		commastr = "delete from DHN_POLLING_RESULT where msg_id in ("
		for i := 1; i < len(fupmsgids); i++ {
			commastr = commastr + "?,"
		}
		commastr = commastr + "?)"
		_, err1 = db.Exec(commastr, fupmsgids...)
		if err1 != nil {
			errlog.Println("Result Table F Delete 처리 중 오류 발생 ")
		}
		fupmsgids = nil 
	}
	  
}
