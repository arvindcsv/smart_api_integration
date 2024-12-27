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
            "exchangeType": 4,
            "tokens": ['1107291', '1107611', '1107673', '1113804', '1113981', '1114183', '1127039', '1137935', '1138012', '1138020', '1138097', '1138121', '1138122', '1138231', '1138290', '1138542', '1138557', '1138625', '1138643', '1138718', '1138729', '1138739', '1138745', '1138803', '1139680', '1140302', '1140382', '1140401', '1140467', '1140663', '1140782', '1140961', '1141045', '1141321', '1141102', '1143204', '1143099', '1143604', '1144027', '1144763', '1144765', '1144838', '1145028', '1145290', '1145374', '1145648', '1145913', '1145962', '1146349', '1146503', '1146512', '1146714', '1146742', '1146913', '1146943', '1147015', '1147027', '1147119', '1147196', '1147198', '1147291', '1147298', '1147406', '1147412', '1147414', '1147495', '1147481', '1147552', '1137953', '1127360', '1107621', '1107152', '1138203', '1138087', '1138029', '1137980', '1138247', '1138252', '1138298', '1138370', '1138432', '1138458', '1138526', '1138579', '1138623', '1138642', '1138689', '1138772', '1138823', '1139870', '1139007', '1139315', '1139957', '1140811', '1140818', '1140233', '1141658', '1141331', '1141200', '1143417', '1143520', '1143570', '1143662', '1143855', '1143995', '1144151', '1144274', '1144277', '1144501', '1144525', '1145142', '1145162', '1145617', '1146929', '1146322', '1146244', '1146995', '1147053', '1147068', '1147084', '1147162', '1147197', '1147230', '1147243', '1147332', '1147284', '1147255', '1147260', '1147513', '1147550', '1147595', '1147456', '1147604', '1147620', '1107426', '1107689', '1113298', '1113488', '1113797', '1137978', '1127051', '1138040', '1138152', '1138293', '1138240', '1138201', '1138478', '1138461', '1138446', '1138319', '1138799', '1138715', '1138602', '1138558', '1139077', '1140392', '1140812', '1140739', '1140949', '1143560', '1143472', '1143369', '1143914', '1143767', '1144103', '1145213', '1145508', '1145277', '1145763', '1146169', '1146428', '1147064', '1147050', '1147011', '1147077', '1147107', '1147135', '1147213', '1147227', '1147276', '1147305', '1147339', '1147352', '1147413', '1147403', '1147404', '1147423', '1147574', '1147564', '1147549', '1147648', '1147705', '1127136', '1113896', '1114271', '1114071', '1127153', '1127158', '1127443', '1137965', '1137973', '1138278', '1138387', '1138217', '1138473', '1138507', '1138536', '1138410', '1138561', '1138609', '1138789', '1138733', '1138893', '1139490', '1139817', '1140024', '1140101', '1140352', '1140624', '1140627', '1141080', '1140985', '1140905', '1141181', '1141597', '1143021', '1143379', '1143412', '1143462', '1143512', '1143626', '1143764', '1143911', '1144458', '1145060', '1144994', '1145427', '1145830', '1145846', '1145723', '1146105', '1145973', '1146303', '1146668', '1146859', '1146755', '1146970', '1147031', '1147124', '1147130']
        },
        {
            "exchangeType": 2,
            "tokens": ['100022', '100023', '100025', '100027', '100109', '100110', '100116', '100134', '100149', '100159', '100163', '100179', '100235', '100240', '100331', '100372', '100385', '100388', '100418', '100543', '100548', '100556', '100644', '100652', '100676', '100679', '100688', '100689', '100710', '100728', '100729', '100756', '100757', '100775', '100781', '100785', '100788', '100876', '100885', '100886', '100964', '101046', '101083', '101088', '101179', '101182', '101183', '101196', '101204', '101209', '101210', '101229', '101340', '101342', '101343', '101345', '101348', '101360', '101397', '101400', '101422', '101433', '101455', '101457', '101463', '101465', '101539', '101543', '101548', '101561', '101667', '101669', '101681', '101570', '101690', '101744', '101799', '101807', '101808', '101826', '101841', '101920', '101933', '101942', '102010', '102208', '102232', '102279', '102281', '102301', '102405', '102406', '102409', '102416', '102431', '102418', '102480', '102490', '102520', '102525', '102526', '102554', '102555', '102625', '102631', '102633', '102736', '102927', '102935', '102952', '103010', '103017', '103033', '103061', '103066', '103071', '103120', '103190', '103191', '103242', '103247', '103250', '103283', '103326', '103339', '103334', '103385', '103341', '103393', '103400', '103431', '103436', '103434', '103444', '103604', '103607', '103610', '103613', '103621', '103622', '103641', '103649', '103743', '103717', '103664', '103703', '103744', '103751', '103755', '103757', '103771', '103781', '103783', '103855', '103892', '103961', '103962', '103964', '103968', '104122', '104127', '104128', '104131', '104138', '104144', '104161', '104163', '104176', '104204', '104210', '104244', '104265', '104270', '104326', '104357', '104352', '104328', '104346', '104373', '104379', '104404', '104407', '104484', '104486', '104516', '104617', '104675', '104705', '104731', '104839', '104863', '104857', '104887', '104888', '104889', '104890', '104948', '104950', '104960', '104969', '104972', '104973', '105035', '105113', '105115', '105125', '105126', '105130', '105138', '105142', '105156', '105225', '105237', '105318', '105325', '105385', '105387', '105446', '105461', '105471', '105515', '105517', '105518', '105529', '105530', '105532', '105575', '105601', '105619', '105620', '105624', '105635', '105701', '105713', '105714', '105720', '105724', '105817', '105856', '105875', '105908', '105939', '105952', '105955', '106019', '106021', '106026', '106028', '106054', '106093']
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
        # logger.info("Ticks: {}".format(message))

        # Invoking lambda asynchronously
        invoke_self_lambda_async(message)
        # close_connection()

    def on_control_message(wsapp, message):
        # logger.info(f"Control Message: {message}")
        print()

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