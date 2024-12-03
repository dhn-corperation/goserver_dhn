package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"database/sql"
	"context"
	"sort"

	config "mycs/src/kaoconfig"
	databasepool "mycs/src/kaodatabasepool"
	"mycs/src/kaosendrequest"
	
	"github.com/takama/daemon"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

const (
	name        = "DHNServer_dhn"
	description = "대형네트웍스 카카오 발송 서버"
)

var dependencies = []string{name+".service"}

var resultTable string

type Service struct {
	daemon.Daemon
}

func (service *Service) Manage() (string, error) {

	usage := "Usage: "+name+" install | remove | start | stop | status"

	if len(os.Args) > 1 {
		command := os.Args[1]
		switch command {
		case "install":
			return service.Install()
		case "remove":
			return service.Remove()
		case "start":
			return service.Start()
		case "stop":
			return service.Stop()
		case "status":
			return service.Status()
		default:
			return usage, nil
		}
	}
	config.Stdlog.Println(name+" resultProc() 실행 시작 -----------------------------")
	resultProc()
	config.Stdlog.Println(name+" resultProc() 실행 끝 -----------------------------")
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)

	for {
		select {
		case killSignal := <-interrupt:
			config.Stdlog.Println("Got signal:", killSignal)
			config.Stdlog.Println("Stoping DB Conntion : ", databasepool.DB.Stats())
			defer databasepool.DB.Close()
			if killSignal == os.Interrupt {
				return "Daemon was interrupted by system signal", nil
			}
			return "Daemon was killed", nil
		}
	}
}

func main() {

	config.InitConfig()

	databasepool.InitDatabase()
	
	var rLimit syscall.Rlimit
	
	rLimit.Max = 50000
    rLimit.Cur = 50000
    
    err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
    
    if err != nil {
        config.Stdlog.Println("Error Setting Rlimit ", err)
    }
    
    err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
    
    if err != nil {
        config.Stdlog.Println("Error Getting Rlimit ", err)
    }
    
    config.Stdlog.Println("Rlimit Final", rLimit)
    
	srv, err := daemon.New(name, description, daemon.SystemDaemon, dependencies...)
	if err != nil {
		config.Stdlog.Println("Error: ", err)
		os.Exit(1)
	}
	service := &Service{srv}
	status, err := service.Manage()
	if err != nil {
		config.Stdlog.Println(status, "\nError: ", err)
		os.Exit(1)
	}
	fmt.Println(status)
}

func resultProc() {
	config.Stdlog.Println(name+" 시작")

	//모든 서비스
	allService := map[string]string{}
	allCtxC := map[string]interface{}{}

	alim_user_list, error := databasepool.DB.Query("select distinct user_id from DHN_CLIENT_LIST where use_flag = 'Y' and alimtalk='Y'")

	isAlim := true
	if error != nil {
		config.Stdlog.Println("알림톡 유저 select 오류 ")
		isAlim = false
	}
	defer alim_user_list.Close()

	alimUser := map[string]string{}
	alimCtxC := map[string]interface{}{}

	if isAlim {
		var user_id sql.NullString
		for alim_user_list.Next() {

			alim_user_list.Scan(&user_id)

			ctx, cancel := context.WithCancel(context.Background())
			go kaosendrequest.AlimtalkProc(user_id.String, ctx)

			alimCtxC[user_id.String] = cancel
			alimUser[user_id.String] = user_id.String

			allCtxC["AL"+user_id.String] = cancel
			allService["AL"+user_id.String] = user_id.String

		}
	}
	rsctx, _ := context.WithCancel(context.Background())
	go kaosendrequest.AlimtalkResendProc(rsctx)

	friend_user_list, error := databasepool.DB.Query("select distinct user_id from DHN_CLIENT_LIST where use_flag = 'Y' and friendtalk='Y'")
	isFriend := true
	if error != nil {
		config.Stdlog.Println("알림톡 유저 select 오류 ")
		isFriend = false
	}
	defer friend_user_list.Close()

	friendUser := map[string]string{}
	friendCtxC := map[string]interface{}{}

	if isFriend {
		var user_id sql.NullString
		for friend_user_list.Next() {

			friend_user_list.Scan(&user_id)

			ctx, cancel := context.WithCancel(context.Background())
			go kaosendrequest.FriendtalkProc(user_id.String, ctx)

			friendCtxC[user_id.String] = cancel
			friendUser[user_id.String] = user_id.String

			allCtxC["FR"+user_id.String] = cancel
			allService["FR"+user_id.String] = user_id.String

		} 
	}
	
	r := gin.New()
	r.Use(gin.Recovery())
	
	serCmd := `DHN Server API
Command :
/astop?uid=dhn   	 	-> 알림톡 process stop.
/arun?uid=dhn    	 	-> 알림톡 process run.
/alist           	 	-> 실행 중인 알림톡 process User List.

/fstop?uid=dhn   	 	-> 친구톡 process stop.
/frun?uid=dhn    	 	-> 친구톡 process run.
/flist           	 	-> 실행 중인 친구톡 process User List.

/all             	 	-> DHNServer process list
/allstop         	 	-> DHNServer process stop
`

	r.GET("/", func(c *gin.Context) {
		c.String(200, serCmd)
	})

	r.GET("/astop", func(c *gin.Context) {
		var uid string
		uid = c.Query("uid")
		temp := alimCtxC[uid]
		if temp != nil {
			cancel := alimCtxC[uid].(context.CancelFunc)
			cancel()
			delete(alimCtxC, uid)
			delete(alimUser, uid)

			delete(allService, "AL"+uid)
			delete(allCtxC, "AL"+uid)

			c.String(200, uid+" 종료 신호 전달 완료")
		} else {
			c.String(200, uid+"는 실행 중이지 않습니다.")
		}

	})

	r.GET("/arun", func(c *gin.Context) {
		var uid string
		uid = c.Query("uid")
		temp := alimCtxC[uid]
		if temp != nil {
			c.String(200, uid+"이미 실행 중입니다.")
		} else {

			ctx, cancel := context.WithCancel(context.Background())
			ctx = context.WithValue(ctx, "user_id", uid)
			go kaosendrequest.AlimtalkProc(uid, ctx)

			alimCtxC[uid] = cancel
			alimUser[uid] = uid

			allCtxC["AL"+uid] = cancel
			allService["AL"+uid] = uid

			c.String(200, uid+" 시작 신호 전달 완료")
		}
	})

	r.GET("/alist", func(c *gin.Context) {
		var key string
		for k := range alimUser {
			key = key + k + "\n"
		}
		c.String(200, key)
	})

	r.GET("/fstop", func(c *gin.Context) {
		var uid string
		uid = c.Query("uid")
		temp := friendCtxC[uid]
		if temp != nil {
			cancel := friendCtxC[uid].(context.CancelFunc)
			cancel()
			delete(friendCtxC, uid)
			delete(friendUser, uid)

			delete(allService, "FR"+uid)
			delete(allCtxC, "FR"+uid)

			c.String(200, uid+" 종료 신호 전달 완료")
		} else {
			c.String(200, uid+"는 실행 중이지 않습니다.")
		}

	})

	r.GET("/frun", func(c *gin.Context) {
		var uid string
		uid = c.Query("uid")
		temp := friendCtxC[uid]
		if temp != nil {
			c.String(200, uid+"이미 실행 중입니다.")
		} else {

			ctx, cancel := context.WithCancel(context.Background())
			ctx = context.WithValue(ctx, "user_id", uid)
			go kaosendrequest.FriendtalkProc(uid, ctx)

			friendCtxC[uid] = cancel
			friendUser[uid] = uid

			allCtxC["FR"+uid] = cancel
			allService["FR"+uid] = uid

			c.String(200, uid+" 시작 신호 전달 완료")
		}
	})

	r.GET("/flist", func(c *gin.Context) {
		var key string
		for k := range friendUser {
			key = key + k + "\n"
		}
		c.String(200, key)
	})

	r.GET("/all", func(c *gin.Context) {
		var key string
		skeys := make([]string, 0, len(allService))
		for k := range allService {
			skeys = append(skeys, k)
		}
		sort.Strings(skeys)
		for _, k := range skeys {
			key = key + k + "\n"
		}
		c.String(200, key)
	})

	r.GET("/allstop", func(c *gin.Context) {
		var key string

		for k := range allService {
			cancel := allCtxC[k].(context.CancelFunc)
			cancel()

			delete(allCtxC, k)
			delete(allService, k)

		}

		c.String(200, key)
	})

	r.Run(":" + config.Conf.PORT)

	config.Stdlog.Println(name+" 실행 포트 : ", config.Conf.PORT)
}
