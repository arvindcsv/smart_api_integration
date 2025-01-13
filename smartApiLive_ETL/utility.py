
import boto3
import time
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer


def dynamodb_deserializer(image):
    """ To Deserialize Dynamodb JSON received from dynamodb stream event """
    return TypeDeserializer().deserialize({'M': image})


def dynamodb_batch_serializer(batch_of_images):
    """ To serialize list(batch) of JSONS into dynamoDB JSON"""
    serialized_batch = []
    for image in batch_of_images:
        serialized_batch.append(
            TypeSerializer().serialize(image)['M']
        )

    return serialized_batch


def batch_insert_into_dynamo(request_items: dict):
    """

    :param request_items:
    :return:
    """

    client = boto3.client('dynamodb', region_name='ap-south-1')

    resp_json = client.batch_write_item(
        RequestItems=request_items
    )

    return resp_json


def prepare_dynamo_batch(records, event):
    """
    In this function we are adding ttl on each record.
    :param records:

    """

    failed_records = []
    # for msg in records:
    # preparing dynamoDB requests for batchWrite
    request_items = {
        f"{event['dynamo_table_name']}": [
            {
                'PutRequest': {
                    'Item': serialized_image
                }
            }
            for serialized_image in records
        ]
    }

    # Batch write into the dynamodb.
    ops_response = batch_insert_into_dynamo(request_items=request_items)
    if ops_response['ResponseMetadata']['HTTPStatusCode'] != 200:
        failed_records.append(request_items)

    # Exponential Back off to retry dynamoDB insert
    cnt = 0
    sleep_time = [0, 10, 20, 30]
    while failed_records and cnt < len(sleep_time):
        time.sleep(sleep_time[cnt])
        for msg in failed_records:
            failed_records.pop(msg)
            ops_response = batch_insert_into_dynamo(request_items=msg)
            if ops_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                failed_records.append(msg)
        cnt += 1  # if any items failed to be loaded, they are returned in this key in same format.

    # Logging onto the failed records
    if failed_records:
        return failed_records
    else:
        return 200