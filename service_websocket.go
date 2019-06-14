package binance

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

func (as *apiService) DepthWebsocket(dwr DepthWebsocketRequest) (chan *DepthEvent, chan struct{}, error) {
	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@depth", strings.ToLower(dwr.Symbol))
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	done := make(chan struct{})
	dech := make(chan *DepthEvent)

	go func() {
		defer c.Close()
		defer close(done)
		for {
			select {
			case <-as.Ctx.Done():
				level.Info(as.Logger).Log("closing reader")
				return
			default:
				_, message, err := c.ReadMessage()
				if err != nil {
					level.Error(as.Logger).Log("wsRead", err)
					return
				}
				rawDepth := struct {
					Type          string          `json:"e"`
					Time          float64         `json:"E"`
					Symbol        string          `json:"s"`
					UpdateID      int             `json:"U"`
					LastUpdateID  int             `json:"u"`
					BidDepthDelta [][]interface{} `json:"b"`
					AskDepthDelta [][]interface{} `json:"a"`
				}{}
				if err := json.Unmarshal(message, &rawDepth); err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
					return
				}
				t, err := timeFromUnixTimestampFloat(rawDepth.Time)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
					return
				}
				de := &DepthEvent{
					WSEvent: WSEvent{
						Type:   rawDepth.Type,
						Time:   t,
						Symbol: rawDepth.Symbol,
					},
					UpdateID: rawDepth.UpdateID,
				}

				de.OrderBook.LastUpdateID = rawDepth.LastUpdateID

				for _, b := range rawDepth.BidDepthDelta {
					p, err := floatFromString(b[0])
					if err != nil {
						level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
						return
					}
					q, err := floatFromString(b[1])
					if err != nil {
						level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
						return
					}
					de.Bids = append(de.Bids, &Order{
						Price:    p,
						Quantity: q,
					})
				}
				for _, a := range rawDepth.AskDepthDelta {
					p, err := floatFromString(a[0])
					if err != nil {
						level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
						return
					}
					q, err := floatFromString(a[1])
					if err != nil {
						level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
						return
					}
					de.Asks = append(de.Asks, &Order{
						Price:    p,
						Quantity: q,
					})
				}
				dech <- de
			}
		}
	}()

	go as.exitHandler(c, done)
	return dech, done, nil
}

func (as *apiService) KlineWebsocket(kwr KlineWebsocketRequest) (chan *KlineEvent, chan struct{}, error) {
	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@kline_%s", strings.ToLower(kwr.Symbol), string(kwr.Interval))
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	done := make(chan struct{})
	kech := make(chan *KlineEvent)

	go func() {
		defer c.Close()
		defer close(done)
		for {
			select {
			case <-as.Ctx.Done():
				level.Info(as.Logger).Log("closing reader")
				return
			default:
				_, message, err := c.ReadMessage()
				if err != nil {
					level.Error(as.Logger).Log("wsRead", err)
					return
				}
				rawKline := struct {
					Type     string  `json:"e"`
					Time     float64 `json:"E"`
					Symbol   string  `json:"S"`
					OpenTime float64 `json:"t"`
					Kline    struct {
						Interval                 string  `json:"i"`
						FirstTradeID             int64   `json:"f"`
						LastTradeID              int64   `json:"L"`
						Final                    bool    `json:"x"`
						OpenTime                 float64 `json:"t"`
						CloseTime                float64 `json:"T"`
						Open                     string  `json:"o"`
						High                     string  `json:"h"`
						Low                      string  `json:"l"`
						Close                    string  `json:"c"`
						Volume                   string  `json:"v"`
						NumberOfTrades           int     `json:"n"`
						QuoteAssetVolume         string  `json:"q"`
						TakerBuyBaseAssetVolume  string  `json:"V"`
						TakerBuyQuoteAssetVolume string  `json:"Q"`
					} `json:"k"`
				}{}
				if err := json.Unmarshal(message, &rawKline); err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
					return
				}
				t, err := timeFromUnixTimestampFloat(rawKline.Time)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Time)
					return
				}
				ot, err := timeFromUnixTimestampFloat(rawKline.Kline.OpenTime)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.OpenTime)
					return
				}
				ct, err := timeFromUnixTimestampFloat(rawKline.Kline.CloseTime)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.CloseTime)
					return
				}
				open, err := floatFromString(rawKline.Kline.Open)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.Open)
					return
				}
				cls, err := floatFromString(rawKline.Kline.Close)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.Close)
					return
				}
				high, err := floatFromString(rawKline.Kline.High)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.High)
					return
				}
				low, err := floatFromString(rawKline.Kline.Low)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.Low)
					return
				}
				vol, err := floatFromString(rawKline.Kline.Volume)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.Volume)
					return
				}
				qav, err := floatFromString(rawKline.Kline.QuoteAssetVolume)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", (rawKline.Kline.QuoteAssetVolume))
					return
				}
				tbbav, err := floatFromString(rawKline.Kline.TakerBuyBaseAssetVolume)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.TakerBuyBaseAssetVolume)
					return
				}
				tbqav, err := floatFromString(rawKline.Kline.TakerBuyQuoteAssetVolume)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawKline.Kline.TakerBuyQuoteAssetVolume)
					return
				}

				ke := &KlineEvent{
					WSEvent: WSEvent{
						Type:   rawKline.Type,
						Time:   t,
						Symbol: rawKline.Symbol,
					},
					Interval:     Interval(rawKline.Kline.Interval),
					FirstTradeID: rawKline.Kline.FirstTradeID,
					LastTradeID:  rawKline.Kline.LastTradeID,
					Final:        rawKline.Kline.Final,
					Kline: Kline{
						OpenTime:                 ot,
						CloseTime:                ct,
						Open:                     open,
						Close:                    cls,
						High:                     high,
						Low:                      low,
						Volume:                   vol,
						NumberOfTrades:           rawKline.Kline.NumberOfTrades,
						QuoteAssetVolume:         qav,
						TakerBuyBaseAssetVolume:  tbbav,
						TakerBuyQuoteAssetVolume: tbqav,
					},
				}
				kech <- ke
			}
		}
	}()

	go as.exitHandler(c, done)
	return kech, done, nil
}

func (as *apiService) TradeWebsocket(twr TradeWebsocketRequest) (chan *AggTradeEvent, chan struct{}, error) {
	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@aggTrade", strings.ToLower(twr.Symbol))
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	done := make(chan struct{})
	aggtech := make(chan *AggTradeEvent)

	go func() {
		defer c.Close()
		defer close(done)
		for {
			select {
			case <-as.Ctx.Done():
				level.Info(as.Logger).Log("closing reader")
				return
			default:
				_, message, err := c.ReadMessage()
				if err != nil {
					level.Error(as.Logger).Log("wsRead", err)
					return
				}
				rawAggTrade := struct {
					Type         string  `json:"e"`
					Time         float64 `json:"E"`
					Symbol       string  `json:"s"`
					TradeID      int     `json:"a"`
					Price        string  `json:"p"`
					Quantity     string  `json:"q"`
					FirstTradeID int     `json:"f"`
					LastTradeID  int     `json:"l"`
					Timestamp    float64 `json:"T"`
					IsMaker      bool    `json:"m"`
				}{}
				if err := json.Unmarshal(message, &rawAggTrade); err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
					return
				}
				t, err := timeFromUnixTimestampFloat(rawAggTrade.Time)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawAggTrade.Time)
					return
				}

				price, err := floatFromString(rawAggTrade.Price)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawAggTrade.Price)
					return
				}
				qty, err := floatFromString(rawAggTrade.Quantity)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawAggTrade.Quantity)
					return
				}
				ts, err := timeFromUnixTimestampFloat(rawAggTrade.Timestamp)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawAggTrade.Timestamp)
					return
				}

				ae := &AggTradeEvent{
					WSEvent: WSEvent{
						Type:   rawAggTrade.Type,
						Time:   t,
						Symbol: rawAggTrade.Symbol,
					},
					AggTrade: AggTrade{
						ID:           rawAggTrade.TradeID,
						Price:        price,
						Quantity:     qty,
						FirstTradeID: rawAggTrade.FirstTradeID,
						LastTradeID:  rawAggTrade.LastTradeID,
						Timestamp:    ts,
						BuyerMaker:   rawAggTrade.IsMaker,
					},
				}
				aggtech <- ae
			}
		}
	}()

	go as.exitHandler(c, done)
	return aggtech, done, nil
}

func (as *apiService) SingleTradeWebsocket(twr TradeWebsocketRequest) (chan *SingleTradeEvent, chan struct{}, error) {
	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@trade", strings.ToLower(twr.Symbol))
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	done := make(chan struct{})
	stch := make(chan *SingleTradeEvent)

	go func() {
		defer c.Close()
		defer close(done)
		for {
			select {
			case <-as.Ctx.Done():
				level.Info(as.Logger).Log("closing reader")
				return
			default:
				_, message, err := c.ReadMessage()
				if err != nil {
					level.Error(as.Logger).Log("wsRead", err)
					return
				}
				rawSingleTrade := struct {
					Type         string  `json:"e"`
					Time         float64 `json:"E"`
					Symbol       string  `json:"s"`
					TradeID      int     `json:"t"`
					Price        string  `json:"p"`
					Quantity     string  `json:"q"`
					BuyerOrderID int     `json:"b"`
					SellerOrdrID int     `json:"a"`
					Timestamp    float64 `json:"T"`
					BuyerMaker   bool    `json:"m"`
				}{}
				if err := json.Unmarshal(message, &rawSingleTrade); err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", string(message))
					return
				}
				t, err := timeFromUnixTimestampFloat(rawSingleTrade.Time)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawSingleTrade.Time)
					return
				}

				price, err := floatFromString(rawSingleTrade.Price)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawSingleTrade.Price)
					return
				}
				qty, err := floatFromString(rawSingleTrade.Quantity)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawSingleTrade.Quantity)
					return
				}
				ts, err := timeFromUnixTimestampFloat(rawSingleTrade.Timestamp)
				if err != nil {
					level.Error(as.Logger).Log("wsUnmarshal", err, "body", rawSingleTrade.Timestamp)
					return
				}

				ste := &SingleTradeEvent{
					WSEvent: WSEvent{
						Type:   rawSingleTrade.Type,
						Time:   t,
						Symbol: rawSingleTrade.Symbol,
					},
					SingleTrade: SingleTrade{
						ID:            rawSingleTrade.TradeID,
						Price:         price,
						Quantity:      qty,
						BuyerOrderID:  rawSingleTrade.BuyerOrderID,
						SellerOrderID: rawSingleTrade.SellerOrdrID,
						Timestamp:     ts,
						BuyerMaker:    rawSingleTrade.BuyerMaker,
					},
				}
				stch <- ste
			}
		}
	}()

	go as.exitHandler(c, done)
	return stch, done, nil
}

func (as *apiService) UserDataWebsocket(urwr UserDataWebsocketRequest) (*UserDataEventChannels, chan struct{}, error) {
	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s", urwr.ListenKey)
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	done := make(chan struct{})
	udechan := &UserDataEventChannels{
		AccountEventChan:      make(chan *AccountEvent),
		UpdatedOrderEventChan: make(chan *UpdatedOrderEvent),
	}

	go func() {
		defer c.Close()
		defer close(done)
		untypedMsg := make(map[string]interface{})
		for {
			select {
			case <-as.Ctx.Done():
				level.Info(as.Logger).Log("closing reader")
				return
			default:
				_, message, err := c.ReadMessage()
				if err != nil {
					level.Error(as.Logger).Log("wsRead", err)
					return
				}

				if err := json.Unmarshal(message, &untypedMsg); err != nil {
					level.Error(as.Logger).Log("wsRawMessageDecode", err)
					return
				}

				switch untypedMsg["e"] {
				case "outboundAccountInfo":
					achan, err := as.mapAccountEvent(untypedMsg)
					if err != nil {
						level.Error(as.Logger).Log("wsAccountEventDecode", err)
						return
					}
					udechan.AccountEventChan <- achan
				case "executionReport":
					uochan, err := as.mapUpdatedOrderEvent(untypedMsg)
					if err != nil {
						level.Error(as.Logger).Log("wsUpdatedOrderEventDecode", err)
						return
					}
					udechan.UpdatedOrderEventChan <- uochan
				default:
					level.Error(as.Logger).Log("wsUserDataStreem", fmt.Sprintf("Not implemented event: %s", untypedMsg["e"]))
					return
				}
			}
		}
	}()

	go as.exitHandler(c, done)
	return udechan, done, nil
}

func (as *apiService) mapAccountEvent(untypedMsg map[string]interface{}) (*AccountEvent, error) {
	ae := &AccountEvent{}

	rawAccount := struct {
		Type             string                   `mapstructure:"e"`
		Time             float64                  `mapstructure:"E"`
		MakerCommission  int64                    `mapstructure:"m"`
		TakerCommission  int64                    `mapstructure:"t"`
		BuyerCommission  int64                    `mapstructure:"b"`
		SellerCommission int64                    `mapstructure:"s"`
		CanTrade         bool                     `mapstructure:"T"`
		CanWithdraw      bool                     `mapstructure:"W"`
		CanDeposit       bool                     `mapstructure:"D"`
		Balances         []map[string]interface{} `mapstructure:"B"`
	}{}

	decoderConf := &mapstructure.DecoderConfig{
		Result:           &rawAccount,
		WeaklyTypedInput: true,
	}

	decoder, err := mapstructure.NewDecoder(decoderConf)

	if err != nil {
		return ae, err
	}

	err = decoder.Decode(untypedMsg)

	if err != nil {
		return ae, err
	}

	t, err := timeFromUnixTimestampFloat(rawAccount.Time)
	if err != nil {
		return ae, errors.Wrapf(err, "AccountEventMappingError (field Time)")
	}

	ae.WSEvent = WSEvent{
		Type: rawAccount.Type,
		Time: t,
	}

	ae.Account = Account{
		MakerCommision:  rawAccount.MakerCommission,
		TakerCommision:  rawAccount.TakerCommission,
		BuyerCommision:  rawAccount.BuyerCommission,
		SellerCommision: rawAccount.SellerCommission,
		CanTrade:        rawAccount.CanTrade,
		CanWithdraw:     rawAccount.CanWithdraw,
		CanDeposit:      rawAccount.CanDeposit,
	}

	for _, b := range rawAccount.Balances {
		free, err := floatFromString(b["f"].(string))
		if err != nil {
			return ae, errors.Wrapf(err, "AccountEventMappingError (field f)")
		}
		locked, err := floatFromString(b["l"].(string))
		if err != nil {
			return ae, errors.Wrapf(err, "AccountEventMappingError (field l)")
		}
		asset, ok := b["a"].(string)
		if !ok {
			return ae, fmt.Errorf("AccountEventMappingError (field a). Asset is not string")
		}
		ae.Balances = append(ae.Balances, &Balance{
			Asset:  asset,
			Free:   free,
			Locked: locked,
		})
	}
	return ae, nil
}

func (as *apiService) mapUpdatedOrderEvent(untypedMsg map[string]interface{}) (*UpdatedOrderEvent, error) {
	uoe := &UpdatedOrderEvent{}

	rawUpdatedOrder := struct {
		Type                              string  `mapstructure:"e"` // Event type
		Time                              float64 `mapstructure:"E"` // Event time
		Symbol                            string  `mapstructure:"s"` // Symbol
		ClientOrderID                     string  `mapstructure:"c"` // Client order ID
		Side                              string  `mapstructure:"S"` // Side
		OrderType                         string  `mapstructure:"o"` // Order type
		TimeInForce                       string  `mapstructure:"f"` // Time in force
		Qty                               float64 `mapstructure:"q"` // Order quantity
		Price                             float64 `mapstructure:"p"` // Order price
		StopPrice                         float64 `mapstructure:"P"` // Stop price
		IcebergQty                        float64 `mapstructure:"F"` // Iceberg quantity
		OriginalClientOrderID             string  `mapstructure:"C"` // Original client order ID; This is the ID of the order being canceled
		CurrentExecutionType              string  `mapstructure:"x"` // Current execution type
		CurrentOrderStatus                string  `mapstructure:"X"` // Current order status
		OrderRejectReason                 string  `mapstructure:"r"` // Order reject reason; will be an error code.
		OrderID                           int64   `mapstructure:"i"` // Order ID
		LastExecutedQty                   float64 `mapstructure:"l"` // Last executed quantity
		CumulativeFilledQty               float64 `mapstructure:"z"` // Cumulative filled quantity
		LastExecutedPrice                 float64 `mapstructure:"L"` // Last executed price
		CommissionAmount                  float64 `mapstructure:"n"` // Commission amount
		CommissionAsset                   string  `mapstructure:"N"` // Commission asset
		TransactionTime                   float64 `mapstructure:"T"` // Transaction time
		TradeId                           int64   `mapstructure:"t"` // Trade ID
		IsOrderWorking                    bool    `mapstructure:"w"` // Is the order working? Stops will have
		IsMakerSide                       bool    `mapstructure:"m"` // Is this trade the maker side?
		CreatedTime                       float64 `mapstructure:"O"` // Order creation time
		CumulativeQuoteAssetTransactedQty float64 `mapstructure:"Z"` // Cumulative quote asset transacted quantity
		LastQuoteAssetTransactedQty       float64 `mapstructure:"Y"` // Last quote asset transacted quantity (i.e. lastPrice * lastQty)
	}{}

	decoderConf := &mapstructure.DecoderConfig{
		Result:           &rawUpdatedOrder,
		WeaklyTypedInput: true,
	}

	decoder, err := mapstructure.NewDecoder(decoderConf)

	if err != nil {
		return uoe, err
	}

	err = decoder.Decode(untypedMsg)

	if err != nil {
		return uoe, err
	}

	t, err := timeFromUnixTimestampFloat(rawUpdatedOrder.Time)
	if err != nil {
		return uoe, errors.Wrapf(err, "UpdatedOrderEventMappingError")
	}

	tt, err := timeFromUnixTimestampFloat(rawUpdatedOrder.TransactionTime)
	if err != nil {
		return uoe, errors.Wrapf(err, "UpdatedOrderEventMappingError")
	}

	oct, err := timeFromUnixTimestampFloat(rawUpdatedOrder.CreatedTime)
	if err != nil {
		return uoe, errors.Wrapf(err, "UpdatedOrderEventMappingError")
	}

	uoe.WSEvent = WSEvent{
		Type: rawUpdatedOrder.Type,
		Time: t,
	}

	uoe.UpdatedOrder = UpdatedOrder{
		Symbol:                            rawUpdatedOrder.Symbol,
		OrderID:                           rawUpdatedOrder.OrderID,
		ClientOrderID:                     rawUpdatedOrder.ClientOrderID,
		OriginalClientOrderID:             rawUpdatedOrder.OriginalClientOrderID,
		Side:                              OrderSide(rawUpdatedOrder.Side),
		Type:                              OrderType(rawUpdatedOrder.OrderType),
		TimeInForce:                       TimeInForce(rawUpdatedOrder.TimeInForce),
		Qty:                               rawUpdatedOrder.Qty,
		Price:                             rawUpdatedOrder.Price,
		StopPrice:                         rawUpdatedOrder.StopPrice,
		IcebergQty:                        rawUpdatedOrder.IcebergQty,
		CurrentExecutionType:              rawUpdatedOrder.CurrentExecutionType,
		CurrentOrderStatus:                OrderStatus(rawUpdatedOrder.CurrentOrderStatus),
		OrderRejectReason:                 rawUpdatedOrder.OrderRejectReason,
		CumulativeFilledQty:               rawUpdatedOrder.CumulativeFilledQty,
		LastExecutedQty:                   rawUpdatedOrder.LastExecutedQty,
		LastExecutedPrice:                 rawUpdatedOrder.LastExecutedPrice,
		CommissionAmount:                  rawUpdatedOrder.CommissionAmount,
		CommissionAsset:                   rawUpdatedOrder.CommissionAsset,
		TransactionTime:                   tt,
		TradeId:                           rawUpdatedOrder.TradeId,
		IsOrderWorking:                    rawUpdatedOrder.IsOrderWorking,
		IsMakerSide:                       rawUpdatedOrder.IsMakerSide,
		CreatedTime:                       oct,
		CumulativeQuoteAssetTransactedQty: rawUpdatedOrder.CumulativeQuoteAssetTransactedQty,
		LastQuoteAssetTransactedQty:       rawUpdatedOrder.LastQuoteAssetTransactedQty,
	}

	return uoe, nil
}

func (as *apiService) exitHandler(c *websocket.Conn, done chan struct{}) {
	/*
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
	*/
	defer c.Close()

	for {
		select {
		/*
			case t := <-ticker.C:
				err := c.WriteMessage(websocket.TextMessage, []byte(t.String()))
				if err != nil {
					level.Error(as.Logger).Log("wsWrite", err)
					return
				}
		*/
		case <-as.Ctx.Done():
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			level.Info(as.Logger).Log("closing connection")
			return
		}
	}
}
