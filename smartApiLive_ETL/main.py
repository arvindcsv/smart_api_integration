
import fetch_load_dynamo
import utility
import ddbToS3


def lambda_handler(event, context):

    if 'Records' in event:
        response = ddbToS3.main(event['Records'][0]['dynamodb']['NewImage'])
        print()
    else:
        if event['event_type'] == 'FETCH_SMART_API':
            fetch_load_dynamo.main()


if __name__ == '__main__':

    event = {"event_type": "FETCH_SMART_API"}

    dynamo_event = {'Records': [
        {'eventID': 'd5bfcb643b781203a6d22e9ceaff2b1c',
         'eventName': 'INSERT', 'eventVersion': '1.1',
         'eventSource': 'aws:dynamodb', 'awsRegion': 'ap-south-1',
         'dynamodb': {'ApproximateCreationDateTime': 1736687297.0,
                      'Keys': {'symbolToken': {'S': '140791'}},
                      'NewImage': {'lowerCircuit': {'N': '0.05'},
                                   'netChange': {'N': '-2.55'}, 'tradeVolume': {'N': '269675'}, 'percentChange': {'N': '-67.11'}, 'exchFeedTime': {'S': '11-Jan-2025 06:55:04'}, 'avgPrice': {'N': '1.25'}, 'exchTradeTime': {'S': '10-Jan-2025 15:29:30'}, 'ltp': {'N': '1.25'}, 'totSellQuan': {'N': '37275'}, 'upperCircuit': {'N': '21.25'}, 'lastTradeQty': {'N': '175'}, 'high': {'N': '2.95'}, 'depth': {'M': {'buy': {'L': [{'M': {'quantity': {'N': '26775'}, 'price': {'N': '1.2'}, 'orders': {'N': '7'}}}, {'M': {'quantity': {'N': '28525'}, 'price': {'N': '1.15'}, 'orders': {'N': '6'}}}, {'M': {'quantity': {'N': '2625'}, 'price': {'N': '1.1'}, 'orders': {'N': '3'}}}, {'M': {'quantity': {'N': '175'}, 'price': {'N': '1.05'}, 'orders': {'N': '1'}}}, {'M': {'quantity': {'N': '5600'}, 'price': {'N': '1'}, 'orders': {'N': '3'}}}]}, 'sell': {'L': [{'M': {'quantity': {'N': '875'}, 'price': {'N': '1.25'}, 'orders': {'N': '3'}}}, {'M': {'quantity': {'N': '175'}, 'price': {'N': '1.3'}, 'orders': {'N': '1'}}}, {'M': {'quantity': {'N': '175'}, 'price': {'N': '1.35'}, 'orders': {'N': '1'}}}, {'M': {'quantity': {'N': '175'}, 'price': {'N': '1.4'}, 'orders': {'N': '1'}}}, {'M': {'quantity': {'N': '3500'}, 'price': {'N': '1.45'}, 'orders': {'N': '2'}}}]}}}, 'totBuyQuan': {'N': '343175'}, 'low': {'N': '0.95'}, 'process_status': {'S': 'IN_PROGRESS'}, 'exchange': {'S': 'NFO'}, 'opnInterest': {'N': '153825'}, 'tradingSymbol': {'S': 'TCS30JAN253400PE'}, '52WeekLow': {'N': '0'}, 'close': {'N': '3.8'}, 'symbolToken': {'S': '140791'}, '52WeekHigh': {'N': '260'}, 'open': {'N': '2'}}, 'SequenceNumber': '58800000000019642388274', 'SizeBytes': 725, 'StreamViewType': 'NEW_AND_OLD_IMAGES'}, 'eventSourceARN': 'arn:aws:dynamodb:ap-south-1:924479393196:table/tokens_data/stream/2025-01-12T12:37:18.342'}]}

    lambda_handler(dynamo_event, {})