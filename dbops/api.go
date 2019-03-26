package dbops

import (
	"fmt"
	"github.com/anakin/bitmex-kline/swagger"
	"github.com/kataras/iris/core/errors"
	"log"
	"time"
)

type CustomKline struct {
	Id     int
	Ktime  time.Time
	Symbol string
	High   float64
	Low    float64
	Open   float64
	Close  float64
}

func Addkline(tableName string, bin []swagger.TradeBin) error {
	//loc, _ := time.LoadLocation("Asia/Shanghai")
	for _, trade := range bin {
		sql := fmt.Sprintf("insert ignore into %s (ktime,symbol,open,close,high,low,trades,volume,vwap,lastSize,turnover,homeNotional,foreignNotional) values (?,?,?,?,?,?,?,?,?,?,?,?,?)", tableName)
		smtIns, err := dbConn.Prepare(sql)
		if err != nil {
			return err
		}
		//log.Println("time local", trade.Timestamp.In(loc))
		_, err = smtIns.Exec(trade.Timestamp, trade.Symbol, trade.Open, trade.Close, trade.High, trade.Low, trade.Trades, trade.Volume, trade.Vwap, trade.LastSize, trade.Turnover, trade.HomeNotional, trade.ForeignNotional)
		if err != nil {
			return err
		}
		smtIns.Close()
		//make 4h data
		if tableName == "kline_hour" && trade.Timestamp.Hour()%4 == 0 {
			MakeCustomData("kline_hour", "kline_four_hour", 4, trade.Timestamp)
		}

		if tableName == "kline_hour" && trade.Timestamp.Hour()%6 == 0 {
			MakeCustomData("kline_hour", "kline_six_hour", 6, trade.Timestamp)
		}

		if tableName == "kline_hour" && trade.Timestamp.Hour()%12 == 0 {
			MakeCustomData("kline_hour", "kline_twl_hour", 12, trade.Timestamp)
		}
		//make 15m data
		if tableName == "kline_five_min" && trade.Timestamp.Minute()%15 == 0 {
			MakeCustomData("kline_five_min", "kline_fif_min", 3, trade.Timestamp)
		}
		//make 30m data
		if tableName == "kline_five_min" && trade.Timestamp.Minute()%30 == 0 {
			MakeCustomData("kline_five_min", "kline_thir_min", 6, trade.Timestamp)
		}
	}
	return nil
}

func MakeCustomData(fromTable string, toTable string, total int, t time.Time) error {
	customBin, err := GetKline(fromTable, t, total)
	if err != nil {
		log.Println("get kline error,", err.Error())
		return err
	}

	if len(customBin) < total {
		err1 := errors.New("not enough")
		log.Println("kline count not enough")
		return err1
	}

	high := customBin[0].High
	low := customBin[0].Low
	for _, v := range customBin {
		if v.High > high {
			high = v.High
		}
		if v.Low < low {
			low = v.Low
		}
	}
	data := CustomKline{
		Ktime:  customBin[0].Ktime,
		Symbol: customBin[0].Symbol,
		Open:   customBin[total-1].Open,
		Close:  customBin[0].Close,
		High:   high,
		Low:    low,
	}
	AddCustomKline(toTable, data)
	return nil
}
func AddCustomKline(tableName string, data CustomKline) error {
	sql := fmt.Sprintf("insert ignore into %s (ktime,symbol,open,close,high,low) values (?,?,?,?,?,?)", tableName)
	smtIns, err := dbConn.Prepare(sql)
	if err != nil {
		return err
	}
	_, err = smtIns.Exec(data.Ktime, data.Symbol, data.Open, data.Close, data.High, data.Low)
	if err != nil {
		return err
	}
	log.Println("add to ", tableName, ":", data.Ktime)
	smtIns.Close()
	return nil
}

func GetKline(table string, t time.Time, total int) ([]*CustomKline, error) {
	var (
		out                    []*CustomKline
		open, close, high, low float64
		symbol                 string
		ktime                  time.Time
	)
	sql := fmt.Sprintf("select ktime,symbol,open,close,high,low from %s where ktime <=? order by ktime desc limit ?", table)
	stmtOut, err := dbConn.Prepare(sql)
	if err != nil {
		log.Println("get kline error:", err.Error())
		return nil, err
	}
	defer stmtOut.Close()
	row, err := stmtOut.Query(t, total)
	if err != nil {
		log.Println("query kline error:", err.Error())
	}
	for row.Next() {
		err1 := row.Scan(&ktime, &symbol, &open, &close, &high, &low)
		if err1 != nil {
			log.Println("return kline error:", err1.Error())
			break
		}
		m := CustomKline{
			Ktime:  ktime,
			Symbol: symbol,
			Open:   open,
			Close:  close,
			High:   high,
			Low:    low,
		}
		out = append(out, &m)
	}
	return out, nil
}

func GetAllKline(table string) ([]*CustomKline, error) {
	var (
		out                    []*CustomKline
		open, close, high, low float64
		symbol                 string
		ktime                  time.Time
	)
	sql := fmt.Sprintf("select ktime,symbol,open,close,high,low from %s order by ktime desc", table)
	stmtOut, err := dbConn.Prepare(sql)
	if err != nil {
		log.Println("get kline error:", err.Error())
		return nil, err
	}
	defer stmtOut.Close()
	row, err := stmtOut.Query()
	if err != nil {
		log.Println("query kline error:", err.Error())
	}
	for row.Next() {
		err1 := row.Scan(&ktime, &symbol, &open, &close, &high, &low)
		if err1 != nil {
			log.Println("return kline error:", err1.Error())
			break
		}
		m := CustomKline{
			Ktime:  ktime,
			Symbol: symbol,
			Open:   open,
			Close:  close,
			High:   high,
			Low:    low,
		}
		out = append(out, &m)
	}
	return out, nil
}
