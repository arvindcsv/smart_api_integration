
# orderparams = {
#     "variety": "NORMAL",
#     "tradingsymbol": "SBIN-EQ",
#     "symboltoken": "3045",
#     "transactiontype": "BUY",
#     "exchange": "NSE",
#     "ordertype": "LIMIT",
#     "producttype": "INTRADAY",
#     "duration": "DAY",
#     "price": "19500",
#     "squareoff": "0",
#     "stoploss": "0",
#     "quantity": "1"
# }
# # Method 1: Place an order and return the order ID
# orderid = smartApi.placeOrder(orderparams)
# logger.info(f"PlaceOrder : {orderid}")
# # Method 2: Place an order and return the full response
# response = smartApi.placeOrderFullResponse(orderparams)
# logger.info(f"PlaceOrder : {response}")

# modifyparams = {
#     "variety": "NORMAL",
#     "orderid": orderid,
#     "ordertype": "LIMIT",
#     "producttype": "INTRADAY",
#     "duration": "DAY",
#     "price": "19500",
#     "quantity": "1",
#     "tradingsymbol": "SBIN-EQ",
#     "symboltoken": "3045",
#     "exchange": "NSE"
# }
# smartApi.modifyOrder(modifyparams)
# logger.info(f"Modify Orders : {modifyparams}")
#
# smartApi.cancelOrder(orderid, "NORMAL")
#
# orderbook=smartApi.orderBook()
# logger.info(f"Order Book: {orderbook}")
#
# tradebook=smartApi.tradeBook()
# logger.info(f"Trade Book : {tradebook}")
#
# rmslimit=smartApi.rmsLimit()
# logger.info(f"RMS Limit : {rmslimit}")
#
# pos=smartApi.position()
# logger.info(f"Position : {pos}")
#
# holdings=smartApi.holding()
# logger.info(f"Holdings : {holdings}")
#
# allholdings=smartApi.allholding()
# logger.info(f"AllHoldings : {allholdings}")
#
# exchange = "NSE"
# tradingsymbol = "SBIN-EQ"
# symboltoken = 3045
# ltp=smartApi.ltpData("NSE", "SBIN-EQ", "3045")
# logger.info(f"Ltp Data : {ltp}")

mod e ="FULL"
exchangeToken s= {
    "NSE": [
        "3045"
    ]
}
marketDat a =smartApi.getMarketData(mode, exchangeTokens)
logger.info(f"Market Data : {marketData}")

exchange = "BSE"
searchscrip = "Titan"
searchScripData = smartApi.searchScrip(exchange, searchscrip)
logger.info(f"Search Scrip Data : {searchScripData}")
