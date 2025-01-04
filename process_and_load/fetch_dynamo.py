
import traceback
from boto3.dynamodb.conditions import Key, Attr
import boto3


def fetch_main(event):
    partition_key = event['partition_key']
    sort_key = event['sort_key']

    dynamo_resource = boto3.resource("dynamodb",
                                     aws_access_key_id='AKIA5OP2EBWWOQAH3UEQ',
                                     aws_secret_access_key='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',
                                     region_name="ap-south-1")
    table = dynamo_resource.Table('new_table')

    try:
        response = table.query(
            KeyConditionExpression=Key('token').eq(partition_key) & Key('timestamp').begins_with(sort_key),
            # FilterExpression=Attr('timestamp').begins_with(sort_key)
        )
        items = response['Items']
        return {'status_code': 200, 'records': items}
    except Exception as e:
        print(f"Error: {e}")
        return {'status_code': 400, 'exception': traceback.format_exc()}


