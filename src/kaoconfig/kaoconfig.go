package kaoconfig

import (
	"fmt"
	"log"
	"os"
	"time"

	ini "github.com/BurntSushi/toml"
	"github.com/go-resty/resty/v2"
	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
)

type Config struct {
	DB              string
	DBURL           string
	PORT            string
	PROFILE_KEY     string
	API_SERVER      string
	CENTER_SERVER   string
	IMAGE_SERVER    string
	CHANNEL         string
	RESPONSE_METHOD string
	SENDLIMIT       int
	PHONE_MSG_FLAG  string
	DEBUG           string
}

var Conf Config
var Stdlog *log.Logger
var BasePath string
var IsRunning bool = true
var ResultLimit int = 1000
var Client *resty.Client

func InitConfig() {
	path := "/root/DHNServer_dhn/log/DHNServer"
	//path := "./log/DHNServer"
	loc, _ := time.LoadLocation("Asia/Seoul")
	writer, err := rotatelogs.New(
		fmt.Sprintf("%s-%s.log", path, "%Y-%m-%d"),
		rotatelogs.WithLocation(loc),
		rotatelogs.WithMaxAge(-1),
		rotatelogs.WithRotationCount(7),
	)

	if err != nil {
		log.Fatalf("Failed to Initialize Log File %s", err)
	}

	log.SetOutput(writer)
	stdlog := log.New(os.Stdout, "INFO -> ", log.Ldate|log.Ltime)
	stdlog.SetOutput(writer)
	Stdlog = stdlog

	Conf = readConfig()
    BasePath = "/root/DHNServer_dhn/"
	Client = resty.New()

}

func readConfig() Config {
	var configfile = "/root/DHNServer_dhn/config.ini"
	//var configfile = "./config.ini"
	_, err := os.Stat(configfile)
	if err != nil {
		fmt.Println("Config file is missing : ", configfile)
	}

	var result Config
	_, err1 := ini.DecodeFile(configfile, &result)

	if err1 != nil {
		fmt.Println("Config file read error : ", err1)
	}

	return result
}

func InitCenterConfig() {
	path := "/root/DHNCenter_dhn/log/DHNCenter"
//	path := "./log/DHNCenter"
	loc, _ := time.LoadLocation("Asia/Seoul")
	writer, err := rotatelogs.New(
		fmt.Sprintf("%s-%s.log", path, "%Y-%m-%d"),
		rotatelogs.WithLocation(loc),
		rotatelogs.WithMaxAge(-1),
		rotatelogs.WithRotationCount(7),
	)

	if err != nil {
		log.Fatalf("Failed to Initialize Log File %s", err)
	}

	log.SetOutput(writer)
	stdlog := log.New(os.Stdout, "INFO -> ", log.Ldate|log.Ltime)
	stdlog.SetOutput(writer)
	Stdlog = stdlog

	Conf = readCenterConfig()
	BasePath = "/root/DHNCenter_dhn/"
	//Client = resty.New()

}

func readCenterConfig() Config {
	var configfile = "/root/DHNCenter_dhn/config.ini"
//	var configfile = "./config.ini"
	_, err := os.Stat(configfile)
	if err != nil {
		fmt.Println("Config file is missing : ", configfile)
	}

	var result Config
	_, err1 := ini.DecodeFile(configfile, &result)

	if err1 != nil {
		fmt.Println("Config file read error : ", err1)
	}

	return result
}
