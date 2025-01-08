package databasepool

import (
	"log"
	"time"
	"database/sql"

	config "mycs/src/kaoconfig"

	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

func InitDatabase() {
	db, err := sql.Open(config.Conf.DB, config.Conf.DBURL)
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxIdleConns(350)
	db.SetMaxOpenConns(700)
	db.SetConnMaxIdleTime(1 * time.Minute)

	DB = db

}
