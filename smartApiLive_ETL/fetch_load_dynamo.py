
from SmartApi.smartConnect import SmartConnect
import pyotp
import time
import pandas as pd
import utility
import boto3
from decimal import Decimal
import json


def main():
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
        print("Invalid Token: The provided token is not valid.")
        raise e

    correlation_id = "abcde"
    data = {'status': False}
    while data.get('status') == False:
        time.sleep(0.001)

        data = smartApi.generateSession(username, pwd, totp)
        print("Generate Session Checked")

    mode = "FULL"
    exchangeTokens = {
        "NFO": [
            '140802', '140806', '140816', '140820', '143364', '143366', '143432', '143439', '143443', '143435',
            '140804', '140800', '140791', '140821', '140789', '140792', '140803', '140823', '140819', '143369',
            '143377', '143440', '143429', '143447', '140809', '140815', '140826', '143362', '143367', '143428',
            '143422', '143423', '140805', '140801', '140798', '140818', '140811', '143363', '143372', '143379',
            '143380', '143419', '143425', '143433', '143451', '143441', '143444', '140799', '140794', '140796',
        ]
    }
    marketData = smartApi.getMarketData(mode, exchangeTokens)

    if marketData.get('message') == 'SUCCESS':
        records = marketData['data']['fetched']

        dynamodb_obj = boto3.resource('dynamodb',
                                      aws_access_key_id='AKIA5OP2EBWWOQAH3UEQ',
                                      aws_secret_access_key='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',
                                      region_name="ap-south-1")

        dynamodb_table = dynamodb_obj.Table('tokens_data')

        for record in records:
            item = json.loads(json.dumps(record), parse_float=Decimal)
            item['process_status'] = 'IN_PROGRESS'
            dynamodb_table.put_item(
                Item=item
            )
        print()
    return 200

