

import create_table


def lambda_handler(event_details, context):

    resp = create_table.create_partition_for_symbols(event_details)
    print()


if __name__ == '__main__':

    event = {"exchange_type": "nse_fo",
             "tokens": [""]}
    lambda_handler(event, {})
