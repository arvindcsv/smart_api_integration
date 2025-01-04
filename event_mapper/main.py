
from datetime import datetime, timedelta, timezone


def lambda_handler(event):

    current_datetime = datetime.now(tz=timezone(timedelta(hours=5.5))).strftime("%Y-%m-%d %H:%M")

    # hour = current_datetime.split(' ')[1].split(':')[0]
    # min = current_datetime.split(' ')[1].split(':')[1]

    # start_time = hour + ':' + min + ':00'
    # end_time = hour + ':' + min + ':59'

    payload = []
    for token in event:
        payload.append({'partition_key': token, 'sort_key': current_datetime})

    print()



event = ['arvind', 'hemant', 'bondkar']
lambda_handler(event)
