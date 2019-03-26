package dbops

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dbConn *sql.DB
	err    error
)

func init() {
	if dbConn, err = sql.Open("mysql", "binpay_rpc_user:binpay_rpc_passwd@tcp(172.26.13.221:3306)/bitmex?charset=utf8&parseTime=true"); err != nil {
		//if dbConn, err = sql.Open("mysql", "dbuser:dbpwd@tcp(192.168.33.10:3306)/bitmex?charset=utf8"); err != nil {
		panic(err.Error())
	}
}
