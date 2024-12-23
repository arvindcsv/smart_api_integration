
import pandas as pd


def quote_message_cleanup(message):

    # For a specific timezone directly using tz_localize, if you don't want the intermediate UTC value.
    message['exchange_timestamp'] = pd.to_datetime(message['exchange_timestamp'], unit='ms').tz_localize('UTC').tz_convert(
        'Asia/Kolkata')

    message['exchange_timestamp'] = message['exchange_timestamp'].strftime('%Y-%m-%d %H:%M:%S')

    # prices has been given in paise, I have converted them into rupees.
    message['last_traded_price'] = (message['last_traded_price'] / 10000000.0)
    message['average_traded_price'] = (message['average_traded_price'] / 10000000.0)
    message['open_price_of_the_day'] = (message['open_price_of_the_day'] / 10000000.0)
    message['high_price_of_the_day'] = (message['high_price_of_the_day'] / 10000000.0)
    message['low_price_of_the_day'] = (message['low_price_of_the_day'] / 10000000.0)
    message['closed_price'] = (message['closed_price'] / 10000000.0)

    # pop unwanted records
    message.pop('subscription_mode')
    message.pop('subscription_mode_val')
    message.pop('sequence_number')

    return message


def main(event):

    return quote_message_cleanup(event)
