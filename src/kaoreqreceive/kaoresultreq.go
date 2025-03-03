package kaoreqreceive

import (
	//"encoding/json"
	"database/sql"
	//"fmt"

	_ "github.com/go-sql-driver/mysql"

	config "mycs/src/kaoconfig"
	databasepool "mycs/src/kaodatabasepool"

	//"kaoreqtable"

	"github.com/gin-gonic/gin"

	//"strconv"
	s "strings"
)

func Resultreq(c *gin.Context) {

	//stdlog := config.Stdlog
	errlog := config.Stdlog
	db := databasepool.DB

	userid := c.Request.Header.Get("userid")
	userip := c.ClientIP()
	isValidation := false

	sqlstr := "select use_flag, send_limit from DHN_CLIENT_LIST where user_id = '" + userid + "' and ip ='" + userip + "' and use_flag = 'Y'  "
	//	fmt.Println(sqlstr)

	val, verr := databasepool.DB.Query(sqlstr)

	if verr != nil {
		errlog.Println(verr)
	}
	defer val.Close()

	var use_flag, send_limit sql.NullString

    if val != nil {
		val.Next()
		val.Scan(&use_flag, &send_limit)
	
		if s.EqualFold(use_flag.String, "Y") {
			isValidation = true
		}
    } 
	if isValidation {

		db2json := map[string]string{
			"msgid":         "msgid",
			"userid":        "userid",
			"ad_flag":       "ad_flag",
			"button1":       "button1",
			"button2":       "button2",
			"button3":       "button3",
			"button4":       "button4",
			"button5":       "button5",
			"code":          "code",
			"image_link":    "image_link",
			"image_url":     "image_url",
			"kind":          "kind",
			"message":       "message",
			"message_type":  "message_type",
			"msg":           "msg",
			"msg_sms":       "msg_sms",
			"only_sms":      "only_sms",
			"p_com":         "p_com",
			"p_invoice":     "p_invoice",
			"phn":           "phn",
			"profile":       "profile",
			"reg_dt":        "reg_dt",
			"remark1":       "remark1",
			"remark2":       "remark2",
			"remark3":       "remark3",
			"remark4":       "remark4",
			"remark5":       "remark5",
			"res_dt":        "res_dt",
			"reserve_dt":    "reserve_dt",
			"result":        "result",
			"s_code":        "s_code",
			"sms_kind":      "sms_kind",
			"sms_lms_tit":   "sms_lms_tit",
			"sms_sender":    "sms_sender",
			"sync":          "sync",
			"tmpl_id":       "tmpl_id",
			"wide":          "wide",
			"send_group":    "send_group",
			"supplement":    "supplement",
			"price":         "price",
			"currency_type": "currency_type",
			"title":         "title",
		}

		sqlstr := "select * from DHN_RESULT where userid = '" + userid + "' and sync='N' and result = 'Y' limit " + send_limit.String

		reqrows, err := db.Query(sqlstr)
		if err != nil {
			errlog.Fatal(sqlstr, err)
		}
		defer reqrows.Close()

		columnTypes, err := reqrows.ColumnTypes()
		if err != nil {
			errlog.Fatal(err)
		}
		count := len(columnTypes)

		finalRows := []interface{}{}
		upmsgids := []interface{}{}

		var isContinue bool
		for reqrows.Next() {
			scanArgs := make([]interface{}, count)

			for i, v := range columnTypes {

				switch v.DatabaseTypeName() {
				case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
					scanArgs[i] = new(sql.NullString)
					break
				case "BOOL":
					scanArgs[i] = new(sql.NullBool)
					break
				case "INT4":
					scanArgs[i] = new(sql.NullInt64)
					break
				default:
					scanArgs[i] = new(sql.NullString)
				}
			}

			err := reqrows.Scan(scanArgs...)
			if err != nil {
				errlog.Fatal(err)
			}

			masterData := map[string]interface{}{}

			for i, v := range columnTypes {

				isContinue = false

				if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
					masterData[db2json[s.ToLower(v.Name())]] = z.Bool
					isContinue = true
				}

				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					masterData[db2json[s.ToLower(v.Name())]] = z.String
					isContinue = true
				}

				if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
					masterData[db2json[s.ToLower(v.Name())]] = z.Int64
					isContinue = true
				}

				if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
					masterData[db2json[s.ToLower(v.Name())]] = z.Float64
					isContinue = true
				}

				if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
					masterData[db2json[s.ToLower(v.Name())]] = z.Int32
					isContinue = true
				}
				if !isContinue {
					masterData[db2json[s.ToLower(v.Name())]] = scanArgs[i]
				}

				if s.EqualFold(v.Name(), "MSGID") {
					upmsgids = append(upmsgids, masterData[db2json[s.ToLower(v.Name())]])
				}
			}

			finalRows = append(finalRows, masterData)

			if len(upmsgids) >= 500 {

				var commastr = "update DHN_RESULT set sync='Y' where MSGID in ("

				for i := 1; i < len(upmsgids); i++ {
					commastr = commastr + "?,"
				}

				commastr = commastr + "?)"

				_, err1 := db.Exec(commastr, upmsgids...)

				if err1 != nil {
					errlog.Println("Result Table Update 처리 중 오류 발생 ")
				}

				upmsgids = nil
			}
		}

		if len(upmsgids) > 0 {

			var commastr = "update DHN_RESULT set sync='Y' where MSGID in ("

			for i := 1; i < len(upmsgids); i++ {
				commastr = commastr + "?,"
			}

			commastr = commastr + "?)"

			_, err1 := db.Exec(commastr, upmsgids...)

			if err1 != nil {
				errlog.Println("Result Table Update 처리 중 오류 발생 ")
			}

			upmsgids = nil
		}
		if len(finalRows) > 0 {
			errlog.Println("결과 전송 ( ", userid, " ) : ", len(finalRows))
			//errlog.Println(finalRows)
		}
		c.JSON(200, finalRows)
	} else {
		c.JSON(404, gin.H{
			"code":    "error",
			"message": "사용자 아이디 확인",
			"userid":  userid,
			"ip":      userip,
		})
	}
}
