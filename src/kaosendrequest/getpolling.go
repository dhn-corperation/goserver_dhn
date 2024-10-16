package kaosendrequest

import (
	//"bytes"
	//"database/sql"
	"encoding/json"
	"fmt"
	kakao "mycs/src/kakaojson"
	config "mycs/src/kaoconfig"

	databasepool "mycs/src/kaodatabasepool"

	//"io/ioutil"
	//"net"
	//"net/http"

	"strconv"
	s "strings"
	"sync"
	//"time"

	"github.com/go-resty/resty/v2"
)

var polprocCnt int

func PollingProc() {
	var wg sync.WaitGroup

	for {
		wg.Add(1)

		go getPollingProcess(&wg)

		wg.Wait()
	}
}

func getPollingProcess(wg *sync.WaitGroup) {

	defer wg.Done()
	//fmt.Println("시작")
	var db = databasepool.DB
	var conf = config.Conf
	var stdlog = config.Stdlog
	var errlog = config.Stdlog

	channel := make(map[string]interface{})
	channel["channel_key"] = conf.CHANNEL

	//jsonstr, _ := json.Marshal(channel)
	//buff := bytes.NewBuffer(jsonstr)

	// req, err := http.NewRequest("POST", conf.API_SERVER+"/v3/"+conf.PROFILE_KEY+"/responseAll", buff)
	// if err != nil {
	// 	errlog.Println(err)
	// 	errlog.Println("메시지 서버 호출 오류")
	// } else {
	// 	//http.DefaultClient.Timeout = time.Minute * 3
	// 	respClient := &http.Client{
	// 		Timeout: time.Second * 20,
	// 	}
	// 	req.Header.Add("Content-type", "application/json")
	// 	//req.Header.Add("channel_key", conf.CHANNEL)

	// 	resp, err := respClient.Do(req)

	//atResClient := resty.New()
	resp, err := config.Client.R().
		SetHeaders(map[string]string{"Content-Type": "application/json", "channel_key": conf.CHANNEL}).
		SetBody(channel).
		Post(conf.API_SERVER + "v3/" + conf.PROFILE_KEY + "/responseAll")

	if err != nil {
		errlog.Println(err)
		errlog.Println("메시지 서버 호출 오류 2")
	} else {

		// defer resp.Body.Close()

		// respBydy, err := ioutil.ReadAll(resp.Body)

		// if err != nil {
		// 	errlog.Println("Body 읽기 실패")

		// } else {

		//str := string(respBydy)

		if resp.StatusCode() == 200 {
			str := resp.Body()
			var kakaoResp kakao.PollingResponse
			json.Unmarshal([]byte(str), &kakaoResp)

			sinsStrs := []string{}
	        sinsValues := []interface{}{}
	        
	        finsStrs := []string{}
	        finsValues := []interface{}{}
	        
	        insquery := `insert IGNORE into DHN_POLLING_RESULT(
msg_id ,
type ,
result_dt) values %s`

			for i, _ := range kakaoResp.Response.Success {
				stdlog.Println("성공 : " + kakaoResp.Response.Success[i].Serial_number[9:len(kakaoResp.Response.Success[i].Serial_number)] + " / " + kakaoResp.Response.Success[i].Received_at)
				//db.Exec("update DHN_RESULT set result = 'Y' where msgid = '" + kakaoResp.Response.Success[i].Serial_number[9:len(kakaoResp.Response.Success[i].Serial_number)] + "'")
				//supmsgids = append(supmsgids, kakaoResp.Response.Success[i].Serial_number[9:len(kakaoResp.Response.Success[i].Serial_number)])
				sinsStrs = append(sinsStrs, "(?,'S',now())");
				sinsValues = append(sinsValues, kakaoResp.Response.Success[i].Serial_number[9:len(kakaoResp.Response.Success[i].Serial_number)])
			}

			for i, _ := range kakaoResp.Response.Fail {
				stdlog.Println("실퍠 : " + kakaoResp.Response.Fail[i].Serial_number[9:len(kakaoResp.Response.Fail[i].Serial_number)] + " / " + kakaoResp.Response.Fail[i].Received_at)
				//db.Exec("update DHN_RESULT set result = 'Y', code = '9999', message = 'ME09' where msgid = '" + kakaoResp.Response.Fail[i].Serial_number[9:len(kakaoResp.Response.Fail[i].Serial_number)] + "'")
				//fupmsgids = append(fupmsgids, kakaoResp.Response.Fail[i].Serial_number[9:len(kakaoResp.Response.Fail[i].Serial_number)])
				finsStrs = append(finsStrs, "(?,'F',now())");
				finsValues = append(finsValues, kakaoResp.Response.Fail[i].Serial_number[9:len(kakaoResp.Response.Fail[i].Serial_number)])

			}
			
			if len(sinsStrs) > 0 {
 			
 				stmt := fmt.Sprintf(insquery, s.Join(sinsStrs, ","))
				//fmt.Println(stmt)
				_, err := db.Exec(stmt, sinsValues...)

				if err != nil {
					stdlog.Println("Polling Result S Insert 처리 중 오류 발생 ", err)
				}

				sinsStrs = nil
				sinsValues = nil

			}
			
			if len(finsStrs) > 0 {
 			
 				stmt := fmt.Sprintf(insquery, s.Join(finsStrs, ","))
				//fmt.Println(stmt)
				_, err := db.Exec(stmt, finsValues...)

				if err != nil {
					stdlog.Println("Polling Result F Insert 처리 중 오류 발생 ", err)
				}

				finsStrs = nil
				finsValues = nil

			}
			
			if kakaoResp.Response_id > 0 {

				//compreq, err1 := http.NewRequest("POST", conf.API_SERVER+"v3/"+conf.PROFILE_KEY+"/response/"+strconv.Itoa(kakaoResp.Response_id)+"/complete", nil)

				atResidClient := resty.New()
				resp, err := atResidClient.R().
					Post(conf.API_SERVER + "v3/" + conf.PROFILE_KEY + "/response/" + strconv.Itoa(kakaoResp.Response_id) + "/complete")

				if err != nil {
					//errlog.Println(err1)
					errlog.Println("알림톡 결과 처리 중 오류 : ", kakaoResp.Response_id, " / ", err, " / ", resp)
				} else {
					stdlog.Println("알림톡 결과 처리 : ", kakaoResp.Response_id, " / ", resp)
				}
			}
		}
		//}

	}
	//respClient.CloseIdleConnections()
	//}

}
