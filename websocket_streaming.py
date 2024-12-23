import threading

from logzero import logger
from SmartApi.smartConnect import SmartConnect
import pyotp
import json
import boto3

api_key = '7fJMgLEb'
username = 'S736247'
pwd = '1289'
smartApi = SmartConnect(api_key)

try:
    token = "E3ARQ3ZUV4U5QWM32NOC2NF5CA"
    # bToken = base64.b32decode(token)
    totp = pyotp.TOTP(token)
    totp = totp.now()
    print()
except Exception as e:
    logger.error("Invalid Token: The provided token is not valid.")
    raise e

# correlation_id can be anything, it's optional field.
correlation_id = "abcd"

# Generate tokens for the everytime.
token_data = smartApi.generateSession(username, pwd, totp)
if token_data['status'] == False:
    logger.error(token_data)
else:
    # logger.info(f"token_data: {token_data}")
    authToken = token_data['data']['jwtToken']
    refreshToken = token_data['data']['refreshToken']
    feedToken = smartApi.getfeedToken()
    # logger.info(f"Feed-Token :{feedToken}")
    res = smartApi.getProfile(refreshToken)
    # logger.info(f"Get Profile: {res}")
    smartApi.generateToken(refreshToken)
    res=res['data']['exchanges']

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

    mode="FULL"
    exchangeTokens= {
    "NSE": [
    "3045"
    ]
    }
    marketData=smartApi.getMarketData(mode, exchangeTokens)
    logger.info(f"Market Data : {marketData}")

    exchange = "BSE"
    searchscrip = "Titan"
    searchScripData = smartApi.searchScrip(exchange, searchscrip)
    logger.info(f"Search Scrip Data : {searchScripData}")

    # # Websocket Programming

    from SmartApi.smartWebSocketV2 import SmartWebSocketV2

    AUTH_TOKEN = authToken
    API_KEY = api_key
    CLIENT_CODE = username
    FEED_TOKEN = feedToken
    # correlation_id = "abc123"
    action = 1
    mode = 2 # 1 (LTP) 2 (Quote) 3 (Snap Quote) 4 (20-Depth)

    # Exchange Type
    # 1(nse_cm)
    #
    # 2(nse_fo)
    #
    # 3(bse_cm)
    #
    # 4(bse_fo)
    #
    # 5(mcx_fo)
    #
    # 7(ncx_fo)
    #
    # 13(cde_fo)
    token_list = [
        {
            "exchangeType": 1,
            "tokens": ['12825']
        }
    ]
    # token_list1 = [
    #     {
    #         "action": 0,
    #         "exchangeType": 1,
    #         "tokens": ["26009"]
    #     }
    # ]

    #retry_strategy=0 for simple retry mechanism
    sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN,max_retry_attempt=2, retry_strategy=0, retry_delay=10, retry_duration=30)

    #retry_strategy=1 for exponential retry mechanism
    # sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN,max_retry_attempt=3, retry_strategy=1, retry_delay=10,retry_multiplier=2, retry_duration=30)

    def invoke_self_lambda_async(payload):
        """
        Function to only invoke self lambda. no response.

        :param payload:
        :return:
        """

        # lambda_client = boto3.client('lambda')
        lambda_client = boto3.client('lambda',
                                     aws_access_key_id='AKIA5OP2EBWWOQAH3UEQ',
                                     aws_secret_access_key='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',
                                     region_name="ap-south-1")
        lambda_resp = lambda_client.invoke(
            FunctionName='arn:aws:lambda:ap-south-1:924479393196:function:testLambda',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )

    def on_data(wsapp, message):
        logger.info("Ticks: {}".format(message))

        # Invoking lambda asynchronously
        invoke_self_lambda_async(message)
        # close_connection()

    def on_control_message(wsapp, message):
        logger.info(f"Control Message: {message}")

    def on_open(wsapp):
        logger.info("on open")
        some_error_condition = False
        if some_error_condition:
            error_message = "Simulated error"
            if hasattr(wsapp, 'on_error'):
                wsapp.on_error("Custom Error Type", error_message)
        else:
            sws.subscribe(correlation_id, mode, token_list)
            # sws.unsubscribe(correlation_id, mode, token_list1)

    def on_error(wsapp, error):
        logger.error(error)

    def on_close(wsapp):
        logger.info("Close")

    def close_connection():
        sws.close_connection()


    # Assign the callbacks.
    # Subscribe to the
    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close

    sws.connect()
    print()
    threading.Thread(target=sws.connect).start()

    sws.on_control_message = on_control_message

    ########################### SmartWebSocket OrderUpdate Sample Code Start Here ###########################
    # from SmartApi.smartWebSocketOrderUpdate import SmartWebSocketOrderUpdate
    # client = SmartWebSocketOrderUpdate(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN)
    # client.connect()
    # ########################### SmartWebSocket OrderUpdate Sample Code End Here ###########################
    # print()