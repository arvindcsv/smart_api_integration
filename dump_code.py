
# orderparams = {
#     "variety": "NORMAL",
#     "tradingsymbol": "SBIN-EQ",
#     "symboltoken": "3045",
#     "transactiontype": "BUY",
#     "exchange": "NSE",
#     "ordertype": "LIMIT",
#     "producttype": "INTRADAY",
#     "duration": "DAY",
#     "price": "19500",
#     "squareoff": "0",
#     "stoploss": "0",
#     "quantity": "1"
# }
# # Method 1: Place an order and return the order ID
# orderid = smartApi.placeOrder(orderparams)
# logger.info(f"PlaceOrder : {orderid}")
# # Method 2: Place an order and return the full response
# response = smartApi.placeOrderFullResponse(orderparams)
# logger.info(f"PlaceOrder : {response}")

# modifyparams = {
#     "variety": "NORMAL",
#     "orderid": orderid,
#     "ordertype": "LIMIT",
#     "producttype": "INTRADAY",
#     "duration": "DAY",
#     "price": "19500",
#     "quantity": "1",
#     "tradingsymbol": "SBIN-EQ",
#     "symboltoken": "3045",
#     "exchange": "NSE"
# }
# smartApi.modifyOrder(modifyparams)
# logger.info(f"Modify Orders : {modifyparams}")
#
# smartApi.cancelOrder(orderid, "NORMAL")
#
# orderbook=smartApi.orderBook()
# logger.info(f"Order Book: {orderbook}")
#
# tradebook=smartApi.tradeBook()
# logger.info(f"Trade Book : {tradebook}")
#
# rmslimit=smartApi.rmsLimit()
# logger.info(f"RMS Limit : {rmslimit}")
#
# pos=smartApi.position()
# logger.info(f"Position : {pos}")
#
# holdings=smartApi.holding()
# logger.info(f"Holdings : {holdings}")
#
# allholdings=smartApi.allholding()
# logger.info(f"AllHoldings : {allholdings}")
#
# exchange = "NSE"
# tradingsymbol = "SBIN-EQ"
# symboltoken = 3045
# ltp=smartApi.ltpData("NSE", "SBIN-EQ", "3045")
# logger.info(f"Ltp Data : {ltp}")

# mod e ="FULL"
# exchangeToken s= {
#     "NSE": [
#         "3045"
#     ]
# }
# marketDat a =smartApi.getMarketData(mode, exchangeTokens)
# logger.info(f"Market Data : {marketData}")
#
# exchange = "BSE"
# searchscrip = "Titan"
# searchScripData = smartApi.searchScrip(exchange, searchscrip)
# logger.info(f"Search Scrip Data : {searchScripData}")

def create_partition_for_profiles(profile_list: list, event):
    """

    :param event:
    :param profile_list:
    :return:
    """

    query = sql.SQL("""SELECT p_id FROM amazon_ads.ad_account_profiles WHERE profile_id in ({sub})""").format(
        sub=sql.SQL(', ').join(sql.Composed([sql.Literal(str(k))]) for k in profile_list)
    )
    records = service_DB_controller.query_controller(query, event)
    # checking if data was not fetched due to validation. On failure, dict with information comes in a list, so records[0]
    if 'validation' in records:
        return log.response_and_log_handler(log_level="ERROR",
                                            operation_body={"detail": records['validation']['detail']},
                                            operation_message="Query validation failed.")
    elif 'operation_status_code' in records:
        # case where an error occurred within query controller
        return records
    else:
        records = records['queried_data']  # get resultant data from query controller

    p_ids = [i['p_id'] for i in records]
    # Names of all Sponsored Ads Reports Tables in DB
    reporting_schema = 'amazon_ads_reporting_v3'
    report_tables = [
        "sb_adgroup_report",
        "sb_ads_report",
        "sb_campaign_report",
        "sb_placement_report",
        "sb_purchased_product_report",
        "sb_search_term_report",
        "sb_targeting_report",
        "sd_adgroup_report",
        "sd_advertised_product_report",
        "sd_campaign_report",
        "sd_matched_target_adgroup_report",
        "sd_matched_target_campaigns_report",
        "sd_matched_target_targeting_report",
        "sd_purchased_product_report",
        "sd_targeting_report",
        "sp_advertised_product_report",
        "sp_campaign_report",
        "sp_purchased_product_report",
        "sp_search_term_report",
        "sp_targeting_report"
    ]
    successful_profiles_and_tables = []
    failed_profiles_and_tables = []
    for p_id in p_ids:
        for table in report_tables:
            query = f"""
            CREATE TABLE IF NOT EXISTS {reporting_schema}.{table}_partition_p_id_{p_id}
            PARTITION OF {reporting_schema}.{table}
            FOR VALUES IN ('{p_id}')
            """
            records = service_DB_controller.query_controller(query, event)

            # checking if data was not fetched due to validation. On failure, dict with information comes in a list, so records[0]
            if 'validation' in records:
                return log.response_and_log_handler(log_level="ERROR",
                                                    operation_body={"detail": records['validation']['detail']},
                                                    operation_message="Query validation failed.")
            elif 'operation_status_code' in records:
                # case where an error occurred within query controller
                return records
            else:
                if 'CREATE TABLE' in records['status_message']:
                    successful_profiles_and_tables.append({"p_id": p_id, "table": table})
                else:
                    failed_profiles_and_tables.append({"p_id": p_id, "table": table})

    if len(failed_profiles_and_tables) > 0 and len(successful_profiles_and_tables) < 1:
        return {"status": "Failed creating partition/s for all profile_ids.",
                "tag": "failure",
                "failed_profiles": failed_profiles_and_tables,
                "successful_profiles": successful_profiles_and_tables}
    elif len(failed_profiles_and_tables) > 0 and len(successful_profiles_and_tables) > 0:
        return {"status": "Partial failure creating partition/s for some profiles.",
                "tag": "partial_failure",
                "failed_profiles": failed_profiles_and_tables,
                "successful_profiles": successful_profiles_and_tables}
    elif len(failed_profiles_and_tables) < 1 and len(successful_profiles_and_tables) > 0:
        return {"status": "Successfully created partition/s for all profiles.",
                "tag": "success",
                "failed_profiles": failed_profiles_and_tables,
                "successful_profiles": successful_profiles_and_tables}