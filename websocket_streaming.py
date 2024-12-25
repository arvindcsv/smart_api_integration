"""
In this module, Fetching live data from smart api using websocket integration.
- Created a trading account on angel one with all integrations.
- Hardcoded all the credentials in code. Because, we are doing this for internal use.
- Generating TOTP using QR code.
- Generate jwtToken and refreshToken these tokens can be expired after 24 hours.
- Get Symbols list using endpoint which has mentioned in symbols_master_list.py
- Using SmartApi package creating connection, and they continuously send us ticks per second.
- Which ever data they are forwarding, i have directly forwarded that record to lambda function.
- Lambda function is processing the record and inserts record into RDS.
"""


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
    # 1(nse_cm), 2(nse_fo), 3(bse_cm), 4(bse_fo), 5(mcx_fo), 7(ncx_fo), 13(cde_fo)
    token_list = [
        {
            "exchangeType": 3,
            "tokens": ['544225', '543578'] # OLAELEC, OLATECH
        }
    ]

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
    print("Stream Ended")
    # threading.Thread(target=sws.connect).start()
    #
    # sws.on_control_message = on_control_message

    ########################### SmartWebSocket OrderUpdate Sample Code Start Here ###########################
    # from SmartApi.smartWebSocketOrderUpdate import SmartWebSocketOrderUpdate
    # client = SmartWebSocketOrderUpdate(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN)
    # client.connect()
    # ########################### SmartWebSocket OrderUpdate Sample Code End Here ###########################
    # print()