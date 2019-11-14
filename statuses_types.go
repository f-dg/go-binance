package binance

// OrderStatus represents order status enum.
type OrderStatus string

// OrderType represents order type enum.
type OrderType string

// OrderSide represents order side enum.
type OrderSide string

// FilterType represents filter type enum.
type FilterType string

var (
	StatusNew             = OrderStatus("NEW")
	StatusPartiallyFilled = OrderStatus("PARTIALLY_FILLED")
	StatusFilled          = OrderStatus("FILLED")
	StatusCancelled       = OrderStatus("CANCELED")
	StatusPendingCancel   = OrderStatus("PENDING_CANCEL")
	StatusRejected        = OrderStatus("REJECTED")
	StatusExpired         = OrderStatus("EXPIRED")

	TypeLimit  = OrderType("LIMIT")
	TypeMarket = OrderType("MARKET")

	SideBuy  = OrderSide("BUY")
	SideSell = OrderSide("SELL")

	FilterLotSize          = FilterType("LOT_SIZE")
	FilterPriceFilter      = FilterType("PRICE_FILTER")
	FilterPercentPrice     = FilterType("PERCENT_PRICE")
	FilterMinNotional      = FilterType("MIN_NOTIONAL")
	FilterIcebergParts     = FilterType("ICEBERG_PARTS")
	FilterMarketLotSize    = FilterType("MARKET_LOT_SIZE")
	FilterMaxNumAlgoOrders = FilterType("MAX_NUM_ALGO_ORDERS")
)
