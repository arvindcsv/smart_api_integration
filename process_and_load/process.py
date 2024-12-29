
import pandas as pd
import struct



LITTLE_ENDIAN_BYTE_ORDER = "<"

# Possible Subscription Mode
LTP_MODE = 1
QUOTE = 2
SNAP_QUOTE = 3
DEPTH = 4

# Exchange Type
NSE_CM = 1
NSE_FO = 2
BSE_CM = 3
BSE_FO = 4
MCX_FO = 5
NCX_FO = 7
CDE_FO = 13

# Subscription Mode Map
SUBSCRIPTION_MODE_MAP = {
    1: "LTP",
    2: "QUOTE",
    3: "SNAP_QUOTE",
    4: "DEPTH"
}

wsapp = None


def _unpack_data(binary_data, start, end, byte_format="I"):
    """
        Unpack Binary Data to the integer according to the specified byte_format.
        This function returns the tuple
    """
    return struct.unpack(LITTLE_ENDIAN_BYTE_ORDER + byte_format, binary_data[start:end])


def _parse_token_value(binary_packet):
    token = ""
    for i in range(len(binary_packet)):
        if chr(binary_packet[i]) == '\x00':
            return token
        token += chr(binary_packet[i])
    return token


def _parse_binary_data(binary_data):
    parsed_data = {
        "subscription_mode": _unpack_data(binary_data, 0, 1, byte_format="B")[0],
        "exchange_type": _unpack_data(binary_data, 1, 2, byte_format="B")[0],
        "token": _parse_token_value(binary_data[2:27]),
        "sequence_number": _unpack_data(binary_data, 27, 35, byte_format="q")[0],
        "exchange_timestamp": _unpack_data(binary_data, 35, 43, byte_format="q")[0],
        "last_traded_price": _unpack_data(binary_data, 43, 51, byte_format="q")[0]
    }
    try:
        parsed_data["subscription_mode_val"] = SUBSCRIPTION_MODE_MAP.get(parsed_data["subscription_mode"])

        if parsed_data["subscription_mode"] in [QUOTE, SNAP_QUOTE]:
            parsed_data["last_traded_quantity"] = _unpack_data(binary_data, 51, 59, byte_format="q")[0]
            parsed_data["average_traded_price"] = _unpack_data(binary_data, 59, 67, byte_format="q")[0]
            parsed_data["volume_trade_for_the_day"] = _unpack_data(binary_data, 67, 75, byte_format="q")[0]
            parsed_data["total_buy_quantity"] = _unpack_data(binary_data, 75, 83, byte_format="d")[0]
            parsed_data["total_sell_quantity"] = _unpack_data(binary_data, 83, 91, byte_format="d")[0]
            parsed_data["open_price_of_the_day"] = _unpack_data(binary_data, 91, 99, byte_format="q")[0]
            parsed_data["high_price_of_the_day"] = _unpack_data(binary_data, 99, 107, byte_format="q")[0]
            parsed_data["low_price_of_the_day"] = _unpack_data(binary_data, 107, 115, byte_format="q")[0]
            parsed_data["closed_price"] = _unpack_data(binary_data, 115, 123, byte_format="q")[0]

        if parsed_data["subscription_mode"] == SNAP_QUOTE:
            parsed_data["last_traded_timestamp"] = _unpack_data(binary_data, 123, 131, byte_format="q")[0]
            parsed_data["open_interest"] = _unpack_data(binary_data, 131, 139, byte_format="q")[0]
            parsed_data["open_interest_change_percentage"] = _unpack_data(binary_data, 139, 147, byte_format="q")[
                0]
            parsed_data["upper_circuit_limit"] = _unpack_data(binary_data, 347, 355, byte_format="q")[0]
            parsed_data["lower_circuit_limit"] = _unpack_data(binary_data, 355, 363, byte_format="q")[0]
            parsed_data["52_week_high_price"] = _unpack_data(binary_data, 363, 371, byte_format="q")[0]
            parsed_data["52_week_low_price"] = _unpack_data(binary_data, 371, 379, byte_format="q")[0]
            best_5_buy_and_sell_data = _parse_best_5_buy_and_sell_data(binary_data[147:347])
            parsed_data["best_5_buy_data"] = best_5_buy_and_sell_data["best_5_sell_data"]
            parsed_data["best_5_sell_data"] = best_5_buy_and_sell_data["best_5_buy_data"]

        if parsed_data["subscription_mode"] == DEPTH:
            parsed_data.pop("sequence_number", None)
            parsed_data.pop("last_traded_price", None)
            parsed_data.pop("subscription_mode_val", None)
            parsed_data["packet_received_time"] = _unpack_data(binary_data, 35, 43, byte_format="q")[0]
            depth_data_start_index = 43
            depth_20_data = _parse_depth_20_buy_and_sell_data(binary_data[depth_data_start_index:])
            parsed_data["depth_20_buy_data"] = depth_20_data["depth_20_buy_data"]
            parsed_data["depth_20_sell_data"] = depth_20_data["depth_20_sell_data"]

        return parsed_data
    except Exception as e:
        print(f"Error occurred during binary data parsing: {e}")
        raise e


def _parse_best_5_buy_and_sell_data(binary_data):

    def split_packets(binary_packets):
        packets = []

        i = 0
        while i < len(binary_packets):
            packets.append(binary_packets[i: i + 20])
            i += 20
        return packets

    best_5_buy_sell_packets = split_packets(binary_data)

    best_5_buy_data = []
    best_5_sell_data = []

    for packet in best_5_buy_sell_packets:
        each_data = {
            "flag": _unpack_data(packet, 0, 2, byte_format="H")[0],
            "quantity": _unpack_data(packet, 2, 10, byte_format="q")[0],
            "price": _unpack_data(packet, 10, 18, byte_format="q")[0],
            "no of orders": _unpack_data(packet, 18, 20, byte_format="H")[0]
        }

        if each_data["flag"] == 0:
            best_5_buy_data.append(each_data)
        else:
            best_5_sell_data.append(each_data)

    return {
        "best_5_buy_data": best_5_buy_data,
        "best_5_sell_data": best_5_sell_data
    }


def _parse_depth_20_buy_and_sell_data(binary_data):
    depth_20_buy_data = []
    depth_20_sell_data = []

    for i in range(20):
        buy_start_idx = i * 10
        sell_start_idx = 200 + i * 10

        # Parse buy data
        buy_packet_data = {
            "quantity": _unpack_data(binary_data, buy_start_idx, buy_start_idx + 4, byte_format="i")[0],
            "price": _unpack_data(binary_data, buy_start_idx + 4, buy_start_idx + 8, byte_format="i")[0],
            "num_of_orders": _unpack_data(binary_data, buy_start_idx + 8, buy_start_idx + 10, byte_format="h")[0],
        }

        # Parse sell data
        sell_packet_data = {
            "quantity": _unpack_data(binary_data, sell_start_idx, sell_start_idx + 4, byte_format="i")[0],
            "price": _unpack_data(binary_data, sell_start_idx + 4, sell_start_idx + 8, byte_format="i")[0],
            "num_of_orders": _unpack_data(binary_data, sell_start_idx + 8, sell_start_idx + 10, byte_format="h")[0],
        }

        depth_20_buy_data.append(buy_packet_data)
        depth_20_sell_data.append(sell_packet_data)

    return {
        "depth_20_buy_data": depth_20_buy_data,
        "depth_20_sell_data": depth_20_sell_data
    }


def quote_message_cleanup(message):

    # For a specific timezone directly using tz_localize, if you don't want the intermediate UTC value.
    message['exchange_timestamp'] = pd.to_datetime(message['exchange_timestamp'], unit='ms').tz_localize('UTC').tz_convert(
        'Asia/Kolkata')

    message['exchange_timestamp'] = message['exchange_timestamp'].strftime('%Y-%m-%d %H:%M:%S')

    # prices has been given in paise, I have converted them into rupees.
    message['last_traded_price'] = (message['last_traded_price'] / 100.0)
    message['average_traded_price'] = (message['average_traded_price'] / 100.0)
    message['open_price_of_the_day'] = (message['open_price_of_the_day'] / 100.0)
    message['high_price_of_the_day'] = (message['high_price_of_the_day'] / 100.0)
    message['low_price_of_the_day'] = (message['low_price_of_the_day'] / 100.0)
    message['closed_price'] = (message['closed_price'] / 100.0)

    # pop unwanted records
    message.pop('subscription_mode')
    message.pop('subscription_mode_val')
    message.pop('sequence_number')

    return message


def main(event):

    return quote_message_cleanup(event)
