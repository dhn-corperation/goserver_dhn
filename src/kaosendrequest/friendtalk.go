package kaosendrequest

import (
	//"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"context"
	s "strings"
	"strconv"
	"sync"
	"time"
	"sync/atomic"

	cm "mycs/src/kaocommon"
	kakao "mycs/src/kakaojson"
	config "mycs/src/kaoconfig"
	databasepool "mycs/src/kaodatabasepool"
)

type resultStr struct {
	Statuscode int
	BodyData   []byte
	Result     map[string]string
}

func FriendtalkProc(ctx context.Context) {
	ftprocCnt := 0
	config.Stdlog.Println("친구톡 프로세스 시작 됨 ")

	for {
		if ftprocCnt < 50 {
		
			select {
			case <- ctx.Done():
			    config.Stdlog.Println("친구톡 process가 10초 후에 종료 됨.")
			    time.Sleep(10 * time.Second)
			    config.Stdlog.Println("친구톡 process 종료 완료")
			    return
			default:
				tx, err := databasepool.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})

				if err != nil {
					config.Stdlog.Println("친구톡 트랜잭션 초기화 실패 : ", err)
					continue
				}

				var startNow = time.Now()
				var group_no = fmt.Sprintf("%02d%02d%02d%09d", startNow.Hour(), startNow.Minute(), startNow.Second(), startNow.Nanosecond()) + strconv.Itoa(ftprocCnt)

				// updateRows, err := databasepool.DB.Exec("update DHN_REQUEST set send_group = ? where send_group is null and userid = ? limit ?", group_no, user_id, strconv.Itoa(config.Conf.SENDLIMIT))
				updateRows, err := tx.Exec("update DHN_REQUEST as a join (select id from DHN_REQUEST where send_group is null limit ?) as b on a.id = b.id set send_group = ?", strconv.Itoa(config.Conf.SENDLIMIT), group_no)

				if err != nil {
					config.Stdlog.Println("친구톡 send_group update 오류 : ", err)
					tx.Rollback()
					continue
				}

				rowCount, err := updateRows.RowsAffected()

				if err != nil {
					config.Stdlog.Println("친구톡 RowsAffected 확인 오류 : ", err)
					tx.Rollback()
					continue
				}

				if rowCount == 0 {
					tx.Rollback()
					time.Sleep(500 * time.Millisecond)
					continue
				}

				if err := tx.Commit(); err != nil {
					config.Stdlog.Println("친구톡 tx Commit 오류 : ", err)
					tx.Rollback()
					continue
				}

				ftprocCnt++
				config.Stdlog.Println("친구톡 발송 처리 시작 ( ", group_no, " ) : ", rowCount, " 건  ( Proc Cnt :", ftprocCnt, ") - START")

				go func() {
					defer func() {
						ftprocCnt--
					}()
					ftsendProcess(group_no, ftprocCnt)
				}()
			}
		}
	}
}

func ftsendProcess(group_no string, pc int) {

	var db = databasepool.DB
	var conf = config.Conf
	var stdlog = config.Stdlog
	var errlog = config.Stdlog

	reqsql := "select * from DHN_REQUEST where send_group = '" + group_no + "'"

	reqrows, err := db.Query(reqsql)
	if err != nil {
		errlog.Fatal(err)
	}
	defer reqrows.Close()

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
	resinsquery := `insert into DHN_RESULT(
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
header,
carousel      
) values %s`


	ftreqinsStrs := []string{}
	ftreqinsValues := []interface{}{}
	ftreqinsQuery := `insert into DHN_REQUEST_RESEND(
msgid,             
userid,            
ad_flag,           
button1,           
button2,           
button3,           
button4,           
button5,           
image_link,        
image_url,         
message_type,      
msg,               
msg_sms,           
only_sms,          
phn,               
profile,           
p_com,             
p_invoice,         
reg_dt,            
remark1,           
remark2,           
remark3,           
remark4,           
remark5,           
reserve_dt,        
sms_kind,          
sms_lms_tit,       
sms_sender,        
s_code,            
tmpl_id,           
wide,              
send_group,        
supplement,        
price,             
currency_type,
title,
header,
carousel,
att_items,
att_coupon,
real_msgid
) values %s`


	resultChan := make(chan resultStr, config.Conf.SENDLIMIT)
	defer close(resultChan)

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

		var friendtalk kakao.Friendtalk
		var attache kakao.Attachment
		var tcarousel kakao.TCarousel
		var carousel kakao.FCarousel
		var button []kakao.Button
		var image kakao.Image
		var coupon kakao.AttCoupon
		var itemList kakao.AttItem
		result := map[string]string{}

		for i, v := range columnTypes {

			switch s.ToLower(v.Name()) {
			case "msgid":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					friendtalk.Serial_number = serial_number + z.String
				}

			case "message_type":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					friendtalk.Message_type = s.ToUpper(z.String)
				}

			case "profile":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					friendtalk.Sender_key = z.String
				}

			case "phn":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					friendtalk.Phone_number = z.String
				}

			case "msg":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					friendtalk.Message = z.String
				}

			case "ad_flag":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					friendtalk.Ad_flag = z.String
				}

			case "header":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					friendtalk.Header = z.String
				}

			case "carousel":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				    
					json.Unmarshal([]byte(z.String), &tcarousel)
					carousel.Tail = tcarousel.Tail
					  
					for ci, _ := range tcarousel.List {
						var catt kakao.CarouselAttachment
						var tcarlist kakao.CarouselList
						
						json.Unmarshal([]byte(tcarousel.List[ci].Attachment), &catt)
						
						tcarlist.Header = tcarousel.List[ci].Header
						tcarlist.Message = tcarousel.List[ci].Message
						tcarlist.Attachment = catt
						carousel.List = append(carousel.List, tcarlist)
					}
					if len(carousel.List) > 0 {
						friendtalk.Carousel = &carousel
					}  
				}

			case "image_url":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					image.Img_url = z.String
				}

			case "image_link":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					image.Img_link = z.String
				}

			case "att_items":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					error := json.Unmarshal([]byte(z.String), &itemList)
					if error == nil {
						attache.Item    = &itemList
					}
				}

			case "att_coupon":
				if z, ok := (scanArgs[i]).(*sql.NullString); ok {
					error := json.Unmarshal([]byte(z.String), &coupon)
					if error == nil {
						attache.Coupon    = &coupon
					}
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

		if len(result["image_url"]) > 0 && s.EqualFold(result["message_type"], "FT") {
			friendtalk.Message_type = "FI"
			if s.EqualFold(result["wide"], "Y") {
				friendtalk.Message_type = "FW"
			}
		}

		attache.Buttons = button
		if len(image.Img_url) > 0 {
			attache.Ftimage = &image
		}
		friendtalk.Attachment = attache
		
		if s.EqualFold(conf.DEBUG,"Y") {
		  	jsonstr, _ := json.Marshal(friendtalk)
			stdlog.Println(string(jsonstr))
		}

		var temp resultStr
		temp.Result = result
		
		reswg.Add(1)
		go sendKakao(&reswg, resultChan, friendtalk, temp)

	}

	reswg.Wait()
	chanCnt := len(resultChan)
	
	nineErrCnt := 0

	for i := 0; i < chanCnt; i++ {

		resChan := <-resultChan
		result := resChan.Result

		if resChan.Statuscode == 200 {

			var kakaoResp kakao.KakaoResponse
			var resdt = time.Now()
			var resdtstr = fmt.Sprintf("%4d-%02d-%02d %02d:%02d:%02d", resdt.Year(), resdt.Month(), resdt.Day(), resdt.Hour(), resdt.Minute(), resdt.Second())

			json.Unmarshal(resChan.BodyData, &kakaoResp)

			var resCode = kakaoResp.Code
			var resMessage = kakaoResp.Message

			resinsStrs = append(resinsStrs, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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

			if s.EqualFold(resCode,"0000") {
				resinsValues = append(resinsValues, "Y") // 
			} else if len(result["sms_kind"])>=1 && s.EqualFold(config.Conf.PHONE_MSG_FLAG, "YES") {
				resinsValues = append(resinsValues, "P") // sms_kind 가 SMS / LMS / MMS 이면 문자 발송 시도
			} else {
				resinsValues = append(resinsValues, "Y") // 
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
			resinsValues = append(resinsValues, result["header"])
			resinsValues = append(resinsValues, result["carousel"])

			if len(resinsStrs) >= 500 {
				resinsStrs, resinsValues = cm.InsMsg(resinsquery, resinsStrs, resinsValues)
			}

		} else if resChan.Statuscode == 500 {
			var kakaoResp2 kakao.KakaoResponse2
			json.Unmarshal(resChan.BodyData, &kakaoResp2)

			var resCode = kakaoResp2.Code

			if s.EqualFold(resCode, "9999"){
				nineErrCnt++
				ftreqinsStrs, ftreqinsValues = insFtErrResend(result, ftreqinsStrs, ftreqinsValues)

				if len(ftreqinsStrs) >= 500 {
					ftreqinsStrs, ftreqinsValues = cm.InsMsg(ftreqinsQuery, ftreqinsStrs, ftreqinsValues)
				}
			}
		} else {
			stdlog.Println("친구톡 서버 처리 오류 : ( ", string(resChan.BodyData), " )", result["msgid"])
			db.Exec("update DHN_REQUEST set send_group = null where msgid = '" + result["msgid"] + "'")
		}

		procCount++
	}

	if len(ftreqinsStrs) > 0 {
		ftreqinsStrs, ftreqinsValues = cm.InsMsg(ftreqinsQuery, ftreqinsStrs, ftreqinsValues)
	}

	if len(resinsStrs) > 0 {
		resinsStrs, resinsValues = cm.InsMsg(resinsquery, resinsStrs, resinsValues)
	}

	// db.Exec("delete from DHN_REQUEST where send_group = '" + group_no + "'")

	stdlog.Println("친구톡 발송 처리 완료 ( ", group_no, " ) : ", procCount, " 건  ( Proc Cnt :", pc, ") - END" )
	

}

func sendKakao(reswg *sync.WaitGroup, c chan<- resultStr, friendtalk kakao.Friendtalk, temp resultStr) {
	defer reswg.Done()

	for {
		currentRL := atomic.LoadInt32(&config.RL)
		if currentRL > 0 {
			if atomic.CompareAndSwapInt32(&config.RL, currentRL, currentRL - 1) {
				break
			}
			// config.RL--
			// break
		}
		time.Sleep(50 * time.Millisecond)
	}

	resp, err := config.Client.R().
		SetHeaders(map[string]string{"Content-Type": "application/json"}).
		SetBody(friendtalk).
		Post(config.Conf.API_SERVER + "v3/" + config.Conf.PROFILE_KEY + "/friendtalk/send")

	if err != nil {
		config.Stdlog.Println("친구톡 메시지 서버 호출 오류 : ", err)
	} else {
		temp.Statuscode = resp.StatusCode()
		temp.BodyData = resp.Body()
	}

	c <- temp
}

func insFtErrResend(result map[string]string, rs []string, rv []interface{}) ([]string, []interface{}) {
	rs = append(rs, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	rv = append(rv, result["msgid"] + "_rs")
	rv = append(rv, result["userid"])
	rv = append(rv, result["ad_flag"])
	rv = append(rv, result["button1"])
	rv = append(rv, result["button2"])
	rv = append(rv, result["button3"])
	rv = append(rv, result["button4"])
	rv = append(rv, result["button5"])
	rv = append(rv, result["image_link"])
	rv = append(rv, result["image_url"])
	rv = append(rv, result["message_type"])
	rv = append(rv, result["msg"])
	rv = append(rv, result["msg_sms"])
	rv = append(rv, result["only_sms"])
	rv = append(rv, result["phn"])
	rv = append(rv, result["profile"])
	rv = append(rv, result["p_com"])
	rv = append(rv, result["p_invoice"])
	rv = append(rv, result["reg_dt"])
	rv = append(rv, result["remark1"])
	rv = append(rv, result["remark2"])
	rv = append(rv, result["remark3"])
	rv = append(rv, result["remark4"])
	rv = append(rv, result["remark5"])
	rv = append(rv, result["reserve_dt"])
	rv = append(rv, result["sms_kind"])
	rv = append(rv, result["sms_lms_tit"])
	rv = append(rv, result["sms_sender"])
	rv = append(rv, result["s_code"])
	rv = append(rv, result["tmpl_id"])
	rv = append(rv, result["wide"])
	rv = append(rv, nil)
	rv = append(rv, result["supplement"])

	if len(result["price"]) > 0 {
		price, _ := strconv.Atoi(result["price"])
		rv = append(rv, price)
	} else {
		rv = append(rv, nil)
	}

	rv = append(rv, result["currency_type"])
	rv = append(rv, result["title"])
    rv = append(rv, result["header"])
    rv = append(rv, result["carousel"])
    rv = append(rv, result["att_items"])
    rv = append(rv, result["att_coupon"])
    rv = append(rv, result["msgid"])

	return rs, rv
}
