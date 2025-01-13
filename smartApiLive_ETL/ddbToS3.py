import pandas as pd
import pyarrow as pa

import utility
from datetime import datetime
import boto3
import io
import pyarrow.parquet as pq


def main(event):

    record = utility.dynamodb_deserializer(event)
    cleaned_record = {}

    # Basic fields
    for key in ['exchange', 'tradingSymbol', 'symbolToken', 'process_status']:
        if key in record:
            cleaned_record[key] = record[key]

    # Decimal fields (handle potential errors)
    decimal_fields = ['lowerCircuit', 'netChange', 'tradeVolume', 'percentChange', 'avgPrice', 'ltp', 'totSellQuan',
                      'upperCircuit', 'lastTradeQty', 'high', 'totBuyQuan', 'low', 'opnInterest', '52WeekLow',
                      'close', '52WeekHigh', 'open']
    for field in decimal_fields:
        try:
            cleaned_record[field] = float(
                record.get(field, 0))  # convert to float for json serializability, default to 0 if missing.
        except (TypeError, ValueError, AttributeError):
            cleaned_record[field] = None  # Or a suitable default value

    # Datetime fields (parse and convert to ISO 8601 strings)
    datetime_fields = ['exchFeedTime', 'exchTradeTime']
    for field in datetime_fields:
        try:
            dt_object = datetime.strptime(record[field], '%d-%b-%Y %H:%M:%S') if record.get(field) else None
            cleaned_record[field] = dt_object.strftime(
                "%Y-%m-%d %H:%M:%S") if dt_object else None  # Format to %Y-%m-%d %H:%M:%S
        except (ValueError, TypeError):
            cleaned_record[field] = None

    # Depth field (clean up nested structure)
    cleaned_record['depth'] = {'buy': [], 'sell': []}
    if 'depth' in record and isinstance(record['depth'], dict):
        for side in ['buy', 'sell']:
            if side in record['depth'] and isinstance(record['depth'][side], list):
                for item in record['depth'][side]:
                    try:
                        cleaned_record['depth'][side].append({
                            'quantity': float(item.get('quantity', 0)),
                            'price': float(item.get('price', 0)),
                            'orders': int(item.get('orders', 0))
                        })
                    except (TypeError, ValueError, AttributeError):
                        continue  # skip invalid data

    _new_df = pd.json_normalize(cleaned_record)

    s3_file_uri = f"s3://smart-api-integrations/NFO/{cleaned_record['symbolToken']}.parquet"
    object_path = f"NFO/{cleaned_record['symbolToken']}.parquet"

    aws_access_key_id = 'AKIA5OP2EBWWOQAH3UEQ'
    aws_secret_access_key = '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'
    region_name = "ap-south-1"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    # Get the Parquet object from S3
    # obj = s3_client.get_object(Bucket='smart-api-integrations', Key=object_path)
    # parquet_buffer = io.BytesIO(obj['Body'].read())
    #
    # # Read the Parquet data into a PyArrow table
    # table = pq.read_table(parquet_buffer)
    #
    # # Convert the PyArrow table to a Pandas DataFrame
    # df = table.to_pandas()

    # print('read parquet')
    # Convert Pandas DataFrame to PyArrow table
    table = pa.Table.from_pandas(_new_df)

    # Write Parquet data to an in-memory buffer
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)  # Important: Reset buffer position

    # Put the Parquet object into S3
    s3_client.put_object(
        Bucket='smart-api-integrations',
        Key=object_path,
        Body=parquet_buffer,
        ContentType='application/x-parquet',
        ACL='public-read-write'
    )
    return cleaned_record
