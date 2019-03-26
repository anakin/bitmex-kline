package main

import (
	"github.com/anakin/bitmex-kline/dbops"
	"github.com/anakin/bitmex-kline/swagger"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

const (
	KLINE_TIME_MIN       = "1m"
	KLINE_TIME_FIVE_MIN  = "5m"
	KLINE_TIME_FIF_MIN   = "15m"
	KLINE_TIME_THIR_MIN  = "30m"
	KLINE_TIME_HOUR      = "1h"
	KLINE_TIME_FOUR_HOUR = "4h"
	KLINE_TIME_SIX_HOUR  = "6h"
	KLINE_TIME_TWL_HOUR  = "12h"
	KLINE_TIME_DAY       = "1d"
)

var (
	apiKey    = "" //TODO your api key here
	secretKey = "" //TODO your api secret key here
	apiClient = swagger.NewAPIClient(swagger.NewConfiguration())
	auth      = context.WithValue(context.TODO(), swagger.ContextAPIKey, swagger.APIKey{
		Key:    apiKey,
		Secret: secretKey,
	})
	tableMap = map[string]string{KLINE_TIME_MIN: "kline_min", KLINE_TIME_FIVE_MIN: "kline_five_min", KLINE_TIME_FIF_MIN: "kline_fif_min", KLINE_TIME_THIR_MIN: "kline_thir_min", KLINE_TIME_HOUR: "kline_hour", KLINE_TIME_FOUR_HOUR: "kline_four_hour", KLINE_TIME_SIX_HOUR: "kline_six_hour", KLINE_TIME_TWL_HOUR: "kline_twl_hour", KLINE_TIME_DAY: "kline_day"}
	interval = []string{KLINE_TIME_FIVE_MIN, KLINE_TIME_HOUR, KLINE_TIME_DAY}
	wg       sync.WaitGroup
)

func main() {
	//klineHistory()
	//customKline()
}

func customKline() error {
	five, err := dbops.GetAllKline("kline_five_min")
	if err != nil {
		return err
	}
	for _, k := range five {
		//make 15m data
		if k.Ktime.Minute()%15 == 0 {
			dbops.MakeCustomData("kline_five_min", "kline_fif_min", 3, k.Ktime)
		}
		//make 30m data
		if k.Ktime.Minute()%30 == 0 {
			dbops.MakeCustomData("kline_five_min", "kline_thir_min", 6, k.Ktime)
		}
	}

	hour, err := dbops.GetAllKline("kline_hour")
	if err != nil {
		return err
	}
	for _, k := range hour {
		if k.Ktime.Hour()%4 == 0 {
			dbops.MakeCustomData("kline_hour", "kline_four_hour", 4, k.Ktime)
		}
		//make 30m data
		if k.Ktime.Hour()%6 == 0 {
			dbops.MakeCustomData("kline_hour", "kline_six_hour", 6, k.Ktime)
		}

		if k.Ktime.Hour()%12 == 0 {
			dbops.MakeCustomData("kline_hour", "kline_twl_hour", 12, k.Ktime)
		}
	}
	return nil
}
func klineHistory() {
	for _, v := range interval {
		wg.Add(1)
		go getKlineData(v)
	}
	wg.Wait()
}

func getKlineData(binSize string) {
	defer wg.Done()
	start := float32(0)
	for {
		log.Println("-----------ticker:", binSize)
		tradeApi := apiClient.TradeApi
		params := map[string]interface{}{
			"binSize":   binSize,
			"symbol":    "XBTUSD",
			"startTime": nil,
			"endTime":   nil,
			"reverse":   true,
			"filter":    "{\"symbol\": \"XBTUSD\"}",
			"columns":   "",
			"start":     start,
			"count":     float32(100),
		}
		bin, _, err := tradeApi.TradeGetBucketed(params)
		if err != nil {
			log.Println("error:", err)
		}
		if len(bin) == 0 {
			break
		}
		tableName := tableMap[binSize]
		err = dbops.Addkline(tableName, bin)
		if err != nil {
			log.Println(err.Error())
		}
		start += float32(100)
		log.Println("get :", binSize, ";total:", len(bin))
		time.Sleep(2 * time.Second)
	}
}
