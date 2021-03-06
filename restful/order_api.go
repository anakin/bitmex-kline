package restful

import (
	"encoding/base64"
	"errors"
	"github.com/anakin/bitmex-kline/swagger"
	"github.com/satori/go.uuid"
	"golang.org/x/net/context"
	"net/http"
	"strings"
)

type OrderApi struct {
	swaggerOrderApi *swagger.OrderApiService
	ctx             context.Context
}

func NewOrderApi(swaggerOrderApi *swagger.OrderApiService, ctx context.Context) *OrderApi {
	return &OrderApi{swaggerOrderApi: swaggerOrderApi, ctx: ctx}
}

func (o *OrderApi) LimitBuy(symbol string, orderQty float64, price float64, clientOrderIDPrefix string) (resp *http.Response, orderId string, err error) {
	if symbol == "" {
		return nil, "", errors.New("symbol can NOT be empty")
	}
	if price <= 0 {
		return nil, "", errors.New("price must be positive")
	}
	clOrdID := ""
	if clientOrderIDPrefix != "" {
		s := strings.Replace(base64.StdEncoding.EncodeToString(uuid.NewV4().Bytes()), "=", "", -1)
		clOrdID = clientOrderIDPrefix + s
	}

	params := map[string]interface{}{
		"symbol":   symbol,
		"ordType":  "Limit",
		"orderQty": float32(orderQty),
		"price":    price,
		"clOrdID":  clOrdID,
	}
	order, response, err := o.swaggerOrderApi.OrderNew(o.ctx, symbol, params)
	if err != nil || response.StatusCode != 200 {
		return response, order.OrderID, err
	}
	return response, order.OrderID, nil
}

func (o *OrderApi) MarketBuy(symbol string, orderQty float64, clientOrderIDPrefix string) (resp *http.Response, orderId string, err error) {
	if symbol == "" {
		return nil, "", errors.New("symbol can NOT be empty")
	}

	clOrdID := ""
	if clientOrderIDPrefix != "" {
		s := strings.Replace(base64.StdEncoding.EncodeToString(uuid.NewV4().Bytes()), "=", "", -1)
		clOrdID = clientOrderIDPrefix + s
	}

	params := map[string]interface{}{
		"symbol":   symbol,
		"ordType":  "Market",
		"orderQty": float32(orderQty),
		"price":    0,
		"clOrdID":  clOrdID,
	}
	order, response, err := o.swaggerOrderApi.OrderNew(o.ctx, symbol, params)
	if err != nil || response.StatusCode != 200 {
		return response, order.OrderID, err
	}
	return response, order.OrderID, nil
}

func (o *OrderApi) LimitSell(symbol string, orderQty float64, price float64, clientOrderIDPrefix string) (resp *http.Response, orderId string, err error) {
	if symbol == "" {
		return nil, "", errors.New("symbol can NOT be empty")
	}
	if price <= 0 {
		return nil, "", errors.New("price must be positive")
	}
	clOrdID := ""
	if clientOrderIDPrefix != "" {
		s := strings.Replace(base64.StdEncoding.EncodeToString(uuid.NewV4().Bytes()), "=", "", -1)
		clOrdID = clientOrderIDPrefix + s
	}

	params := map[string]interface{}{
		"symbol":   symbol,
		"orderQty": float32(-orderQty),
		"price":    price,
		"clOrdID":  clOrdID,
	}
	order, response, err := o.swaggerOrderApi.OrderNew(o.ctx, symbol, params)
	if err != nil || response.StatusCode != 200 {
		return response, order.OrderID, err
	}
	return response, order.OrderID, nil
}
