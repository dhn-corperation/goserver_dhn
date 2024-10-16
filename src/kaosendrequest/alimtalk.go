package kaosendrequest

import (
	//"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	kakao "mycs/src/kakaojson"
	config "mycs/src/kaoconfig"
	databasepool "mycs/src/kaodatabasepool"

	//"io/ioutil"
	//	"net"
	//"net/http"
	"strconv"
	s "strings"
	"sync"
	"time"
)

var atprocCnt int

func AlimtalkProc() {
	atprocCnt = 1
	 
	for {
		if atprocCnt <=5 {
			var startNow = time.Now()
			var group_no = fmt.Sprintf("%02d%02d%02d%09d", startNow.Hour(), startNow.Minute(), startNow.Second(), startNow.Nanosecond())
	
			updateRows, err := databasepool.DB.Exec("update DHN_REQUEST_AT set send_group = '" + group_no + "' where send_group is null limit " + strconv.Itoa(config.Conf.SENDLIMIT))
	
			if err != nil {
				config.Stdlog.Println("알림톡 send_group Update 오류 : ", err)
			}
	
			rowcnt, _ := updateRows.RowsAffected()
	
			if rowcnt > 0 {
				config.Stdlog.Println("알림톡 발송 처리 시작 ( ", group_no, " ) : ", rowcnt, " 건 ")
				atprocCnt++
				go atsendProcess(group_no)
	
			}
		}
	}

}

func atsendProcess(group_no string) {
	var db = databasepool.DB
	var conf = config.Conf
	var stdlog = config.Stdlog
	var errlog = config.Stdlog

	reqsql := "select * from DHN_REQUEST_AT where send_group = '" + group_no + "'  "

	reqrows, err := db.Query(reqsql)
	if err != nil {
		errlog.Fatal(err)
	}

	columnTypes, err := reqrows.ColumnTypes()
	if err != nil {
		errlog.Fatal(err)
	}
	count := len(columnTypes)

	var procCount int
	procCount = 0
	var startNow = time.Now()
	var serial_number = fmt.Sprintf("%04d%02d%02d-", startNow.Year(), startNow.Month(), startNow.Day())

	resinsStrs := []string{}
	resinsValues := []interface{}{}
	resinsquery := `insert IGNORE into DHN_RESULT(
msgid ,
userid ,
ad_flag ,
button1 ,
button2 ,
button3 ,
button4 ,
button5 ,
code ,
image_link ,
image_url ,
kind ,
message ,
message_type ,
msg ,
msg_sms ,
only_sms ,
p_com ,
p_invoice ,
phn ,
profile ,
reg_dt ,
remark1 ,
remark2 ,
remark3 ,
remark4 ,
remark5 ,
res_dt ,
reserve_dt ,
result ,
s_code ,
sms_kind ,
sms_lms_tit ,
sms_sender ,
sync ,
tmpl_id ,
wide ,
send_group ,
supplement ,
price ,
currency_type,
title) values %s`

	resultChan := make(chan resultStr, config.Conf.SENDLIMIT) // resultStr 은 friendtalk에 정의 됨
	var reswg sync.WaitGroup

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

		var alimtalk kakao.Alimtalk
		var attache kakao.AttachmentB
		var supplement kakao.Supplement
		var button []kakao.Button
		var quickreply []kakao.Quickreply
		result := map[string]string{}

		for i, v := range columnTypes {

			switch s.ToLower(v.Name()) {
			case "msgid":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					alimtalk.Serial_number = serial_number + z.String
				}

			case "message_type":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					alimtalk.Message_type = s.ToUpper(z.String)
				}

			case "profile":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					alimtalk.Sender_key = z.String
				}

			case "phn":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					alimtalk.Phone_number = z.String
				}

			case "tmpl_id":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					alimtalk.Template_code = z.String
				}

			case "msg":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					alimtalk.Message = z.String
				}

			case "price":
				if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
					alimtalk.Price = z.Int64
				}

			case "currency_type":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					alimtalk.Currency_type = z.String
				}

			case "title":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					alimtalk.Title = z.String
				}

			case "button1":
				fallthrough
			case "button2":
				fallthrough
			case "button3":
				fallthrough
			case "button4":
				fallthrough
			case "button5":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					if len(z.String) > 0 {
						var btn kakao.Button

						json.Unmarshal([]byte(z.String), &btn)
						button = append(button, btn)
					}
				}
			case "supplement":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					if len(z.String) > 0 {
						var qrp []kakao.Quickreply

						json.Unmarshal([]byte(z.String), &qrp)
						for i, _ := range qrp {
							quickreply = append(quickreply, qrp[i])
						}
					}
				}
			}

			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				result[s.ToLower(v.Name())] = z.String
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				result[s.ToLower(v.Name())] = string(z.Int32)
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				result[s.ToLower(v.Name())] = string(z.Int64)
			}

		}

		if s.EqualFold(result["message_type"], "at") {
			alimtalk.Response_method = "realtime"
		} else if s.EqualFold(result["message_type"], "ai") {
			
			alimtalk.Response_method = conf.RESPONSE_METHOD
			alimtalk.Channel_key = conf.CHANNEL
			
			if  s.EqualFold(conf.RESPONSE_METHOD, "polling") {
				alimtalk.Timeout = 600
			}
			
		}

		attache.Buttons = button
		supplement.Quick_reply = quickreply

		alimtalk.Attachment = attache
		alimtalk.Supplement = supplement

		var temp resultStr
		temp.Result = result
		reswg.Add(1)
		go sendKakaoAlimtalk(&reswg, resultChan, alimtalk, temp)
	}
	reswg.Wait()
	chanCnt := len(resultChan)

	for i := 0; i < chanCnt; i++ {

		resChan := <-resultChan
		result := resChan.Result

		if resChan.Statuscode == 200 {

			var kakaoResp kakao.KakaoResponse
			json.Unmarshal(resChan.BodyData, &kakaoResp)

			var resdt = time.Now()
			var resdtstr = fmt.Sprintf("%4d-%02d-%02d %02d:%02d:%02d", resdt.Year(), resdt.Month(), resdt.Day(), resdt.Hour(), resdt.Minute(), resdt.Second())
			
			var resCode = kakaoResp.Code
			var resMessage = kakaoResp.Message
			
			if s.EqualFold(resCode, "3005") {
				resCode = "0000"
				resMessage = ""
			} 
			
			resinsStrs = append(resinsStrs, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
			resinsValues = append(resinsValues, result["msgid"])
			resinsValues = append(resinsValues, result["userid"])
			resinsValues = append(resinsValues, result["ad_flag"])
			resinsValues = append(resinsValues, result["button1"])
			resinsValues = append(resinsValues, result["button2"])
			resinsValues = append(resinsValues, result["button3"])
			resinsValues = append(resinsValues, result["button4"])
			resinsValues = append(resinsValues, result["button5"])
			resinsValues = append(resinsValues, resCode) // 결과 code
			resinsValues = append(resinsValues, result["image_link"])
			resinsValues = append(resinsValues, result["image_url"])
			resinsValues = append(resinsValues, nil)               // kind
			resinsValues = append(resinsValues, resMessage) // 결과 Message
			resinsValues = append(resinsValues, result["message_type"])
			resinsValues = append(resinsValues, result["msg"])
			resinsValues = append(resinsValues, result["msg_sms"])
			resinsValues = append(resinsValues, result["only_sms"])
			resinsValues = append(resinsValues, result["p_com"])
			resinsValues = append(resinsValues, result["p_invoice"])
			resinsValues = append(resinsValues, result["phn"])
			resinsValues = append(resinsValues, result["profile"])
			resinsValues = append(resinsValues, result["reg_dt"])
			resinsValues = append(resinsValues, result["remark1"])
			resinsValues = append(resinsValues, result["remark2"])
			resinsValues = append(resinsValues, result["remark3"])
			resinsValues = append(resinsValues, result["remark4"])
			resinsValues = append(resinsValues, result["remark5"])
			resinsValues = append(resinsValues, resdtstr) // res_dt
			resinsValues = append(resinsValues, result["reserve_dt"])

			if s.EqualFold(result["message_type"], "at") || !s.EqualFold(resCode, "0000") || (s.EqualFold(result["message_type"], "ai") && s.EqualFold(conf.RESPONSE_METHOD, "push")) {

				if s.EqualFold(resCode,"0000") {
					resinsValues = append(resinsValues, "Y") // 
				} else if len(result["sms_kind"])>=1 && s.EqualFold(config.Conf.PHONE_MSG_FLAG, "YES") {
					resinsValues = append(resinsValues, "P") // sms_kind 가 SMS / LMS / MMS 이면 문자 발송 시도
				} else {
					resinsValues = append(resinsValues, "Y") // 
				} 
				
			} else if s.EqualFold(result["message_type"], "ai") {
				resinsValues = append(resinsValues, "N") // result
			}
			resinsValues = append(resinsValues, result["s_code"])
			resinsValues = append(resinsValues, result["sms_kind"])
			resinsValues = append(resinsValues, result["sms_lms_tit"])
			resinsValues = append(resinsValues, result["sms_sender"])
			resinsValues = append(resinsValues, "N")
			resinsValues = append(resinsValues, result["tmpl_id"])
			resinsValues = append(resinsValues, result["wide"])
			resinsValues = append(resinsValues, nil) // send group
			resinsValues = append(resinsValues, result["supplement"])
			resinsValues = append(resinsValues, result["price"])
			resinsValues = append(resinsValues, result["currency_type"])
			resinsValues = append(resinsValues, result["title"])

			if len(resinsStrs) >= 500 {
				stmt := fmt.Sprintf(resinsquery, s.Join(resinsStrs, ","))
				//fmt.Println(stmt)
				_, err := databasepool.DB.Exec(stmt, resinsValues...)

				if err != nil {
					stdlog.Println("Result Table Insert 처리 중 오류 발생 ", err)
				}

				resinsStrs = nil
				resinsValues = nil
			}
		} else {
			stdlog.Println("알림톡 서버 처리 오류 !! ( ", string(resChan.BodyData), " )", result["msgid"])
			db.Exec("update DHN_REQUEST_AT set send_group = null where msgid = '" + result["msgid"] + "'")
		}

		procCount++
	}

	if len(resinsStrs) > 0 {
		stmt := fmt.Sprintf(resinsquery, s.Join(resinsStrs, ","))

		_, err := databasepool.DB.Exec(stmt, resinsValues...)

		if err != nil {
			stdlog.Println("Result Table Insert 처리 중 오류 발생 ", err)
		}

		resinsStrs = nil
		resinsValues = nil
	}

	db.Exec("delete from DHN_REQUEST_AT where send_group = '" + group_no + "'")

	stdlog.Println("알림톡 발송 처리 완료 ( ", group_no, " ) : ", procCount, " 건 ( Proc Cnt :", atprocCnt, ")")
	
	atprocCnt--
}

func sendKakaoAlimtalk(reswg *sync.WaitGroup, c chan<- resultStr, alimtalk kakao.Alimtalk, temp resultStr) {
	defer reswg.Done()

	resp, err := config.Client.R().
		SetHeaders(map[string]string{"Content-Type": "application/json"}).
		SetBody(alimtalk).
		Post(config.Conf.API_SERVER + "/v3/" + config.Conf.PROFILE_KEY + "/alimtalk/send")

	//fmt.Println("SEND :", resp, err)

	if err != nil {
		config.Stdlog.Println("알림톡 메시지 서버 호출 오류 : ", err)
		//	return nil
	} else {
		temp.Statuscode = resp.StatusCode()
		temp.BodyData = resp.Body()
	}
	c <- temp

}
