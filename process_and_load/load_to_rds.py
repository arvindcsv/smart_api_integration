import db_connection_manager as db_manager
import psycopg
from psycopg.rows import dict_row
from psycopg import sql


exchange_type_mapping = {1: "nse_cm", 2: "nse_fo", 3: "bse_cm", 4: "bse_fo", 5: "mcx_fo", 7: "ncx_fo", 13: "cde_fo"}


def load(_message):

    """
    insert single dictionary into DB table
    :param _message:
    :return:
    """

    # We can't be able to create table name as only in integer.
    table_name = f"{exchange_type_mapping[_message['exchange_type']]}._{_message['token']}"
    _message.pop('exchange_type')
    _message.pop('token')

    conn = db_manager.get_db_connection()
    cursor = conn.cursor()

    # Construct the SQL query dynamically
    columns = ['exchange_timestamp', 'last_traded_price', 'last_traded_quantity',
               'average_traded_price', 'volume_trade_for_the_day', 'total_buy_quantity', 'total_sell_quantity',
               'open_price_of_the_day', 'high_price_of_the_day', 'low_price_of_the_day', 'closed_price']
    columns = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(_message))  # Create placeholders for values
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    values = list(_message.values())

    try:
        cursor.execute(sql, values)
        conn.commit()
        db_manager.put_db_connection(conn)
        return 200
    except (Exception, psycopg.DatabaseError) as e:
        print("Exception:", e)
        conn.rollback()
        db_manager.put_db_connection(conn)
        return 400

