import datetime

import bq_method
import dateutil
import ozon_method
import wb_client
import yaml
from client1c import daterange
from loguru import logger
from ozon_client import OZONApiClient
from simplegui import clean_table_if_necessary, fill_date


def export_orders_from_ozon2bq_updated_in_the_period(
    datarange, bqdataset, bqjsonservicefile, bqtable, configyml, method
):
    apimethods = ozon_method.apimethods
    with open(configyml) as f:
        config = yaml.safe_load(f)
    for lkConfig in config["lks"]:
        ozonid = lkConfig["lk"]["bq_id"]
        apikey = lkConfig["lk"]["apikey"]
        clientid = lkConfig["lk"]["clientid"]

        logger.info(f"Начало импорта из OZON {ozonid}:")
        datefrom = datarange["datefrom"]
        dateto = datarange["dateto"]

        items = ozon_method.ozon_import(
            method,
            apimethods.get(method),
            apikey,
            clientid,
            ozonid,
            datefrom,
            dateto,
            ozon_method.OzonDataFilterType.updated_at,
        )
        if len(items) != 0:
            logger.info(f"Чистим  данные в {bqtable} по {len(items)} заказам")
            fieldname = "operation_date"
            filterList = []
            filterList.append(
                {
                    "fieldname": "ozon_id",
                    "operator": "=",
                    "value": ozonid,
                }
            )
            orderidlist = ""
            for elitems in items:
                if orderidlist != "":
                    orderidlist = orderidlist + ","
                order_id = elitems["posting_number"]
                orderidlist = orderidlist + f"'{order_id}'"

            filterList.append(
                {
                    "fieldname": "posting_number",
                    "operator": " IN ",
                    "value": orderidlist,
                }
            )
            bq_method.DeleteRowFromTable(
                bqtable, bqdataset, bqjsonservicefile, filterList
            )
            fields_list = ozon_method.fields_from_method(method)
            bq_method.export_js_to_bq(
                items, bqtable, bqjsonservicefile, bqdataset, logger, fields_list
            )
            text = f"Всё выгружено {method} c {datefrom} по {dateto}"
        else:
            text = f"Данных нет {method} c {datefrom} по {dateto} - {ozonid}"
            logger.info(text)


def transfer_orders_transaction_ozon2bq_in_the_period(
    daterange,
    bqdataset,
    bqjsonservicefile,
    bqtable,
    configyml,
    fieldname,
    method,
    ozon_data_filter_type,
):
    apimethods = ozon_method.apimethods
    with open(configyml) as f:
        config = yaml.safe_load(f)
    for lkConfig in config["lks"]:
        ozonid = lkConfig["lk"]["bq_id"]
        apikey = lkConfig["lk"]["apikey"]
        clientid = lkConfig["lk"]["clientid"]

        logger.info(f"Начало импорта из OZON {ozonid}:")
        datefrom = daterange["datefrom"]
        dateto = daterange["dateto"]

        try:
            #   js=ozon_method.ozon_import(apimethods.get(method),apikey,LOG_FILE,dateimport,maxdatechange)
            # clientid='44346'

            items = ozon_method.ozon_import(
                method,
                apimethods.get(method),
                apikey,
                clientid,
                ozonid,
                datefrom,
                dateto,
                ozon_data_filter_type,
            )
            if len(items) != 0:
                logger.info(f"Чистим  данные в {bqtable} c {datefrom} по {dateto}")
                pattern_dateto = "%Y-%m-%d"
                if fieldname == "created_at":
                    pattern_dateto = "%Y-%m-%d 23:59:59"

                filterList = []
                filterList.append(
                    {
                        "fieldname": "ozon_id",
                        "operator": "=",
                        "value": ozonid,
                    }
                )

                filterList.append(
                    {
                        "fieldname": fieldname,
                        "operator": ">=",
                        "value": datefrom.strftime("%Y-%m-%d"),
                    }
                )
                filterList.append(
                    {
                        "fieldname": fieldname,
                        "operator": "<=",
                        "value": dateto.strftime(pattern_dateto),
                    }
                )
                bq_method.DeleteRowFromTable(
                    bqtable, bqdataset, bqjsonservicefile, filterList
                )
                fields_list = ozon_method.fields_from_method(method)
                bq_method.export_js_to_bq(
                    items, bqtable, bqjsonservicefile, bqdataset, logger, fields_list
                )
            else:
                logger.info(f"Данных нет {method} c {datefrom} по {dateto}")
        except Exception as e:
            logger.exception("Ошибка выполнения." + e.__str__())


def export_orders_from_ozon2bq_by_id(
    bqdataset, bqjsonservicefile, bqtable, configyml, order_id, ozon_id
):
    method = "orders_v3"
    urimethod = ozon_method.apimethods.get(method)

    with open(configyml) as f:
        config = yaml.safe_load(f)

    filter_list = list(filter(lambda x: x["lk"]["bq_id"] == ozon_id, config["lks"]))
    if len(filter_list) == 0:
        raise f"Не найден {ozon_id} в файле конфигурации {configyml}!"

    for lkConfig in filter_list:
        ozonid = lkConfig["lk"]["bq_id"]
        apikey = lkConfig["lk"]["apikey"]
        clientid = lkConfig["lk"]["clientid"]

        logger.info(f"Начало импорта из OZON {ozonid}:")
        datefrom = datetime.date.today()
        dateto = datetime.date.today()

        items = ozon_method.ozon_import(
            method,
            urimethod,
            apikey,
            clientid,
            ozonid,
            datefrom,
            dateto,
            ozon_method.OzonDataFilterType.order_id,
            order_id,
        )
        if len(items) != 0:
            logger.info(f"Чистим  данные в {bqtable} по {len(items)} заказам")
            fieldname = "operation_date"
            filterList = []
            filterList.append(
                {
                    "fieldname": "ozon_id",
                    "operator": "=",
                    "value": ozonid,
                }
            )
            orderidlist = ""
            for elitems in items:
                if orderidlist != "":
                    orderidlist = orderidlist + ","
                order_id = elitems["order_id"]
                orderidlist = orderidlist + f"'{order_id}'"

            filterList.append(
                {
                    "fieldname": "order_id",
                    "operator": " IN ",
                    "value": orderidlist,
                }
            )
            bq_method.DeleteRowFromTable(
                bqtable, bqdataset, bqjsonservicefile, filterList
            )
            fields_list = ozon_method.fields_from_method(method)
            bq_method.export_js_to_bq(
                items, bqtable, bqjsonservicefile, bqdataset, logger, fields_list
            )
            text = f"Всё выгружено {method} c {datefrom} по {dateto}"
        else:
            text = f"Данных нет {method} c {datefrom} по {dateto} - {ozonid}"
            logger.info(text)


def wb_export(
    method,
    bqtable,
    option="changes",
    jsonkey="polar.json",
    datasetid="wb",
    configyml="config_wb.yml",
    datefromstr="",
    datetostr="",
    datefrom="",
    dateto="",
):
    with open(configyml) as f:
        config = yaml.safe_load(f)

    if method == "reportsale":
        field_date = "rr_dt"
    elif option == "byPeriod":
        field_date = "date"
    elif option == "changes":
        field_date = "lastChangeDate"

    for lkConfig in config["lks"]:
        wb_id = lkConfig["lk"]["bq_id"]
        apikey_v1 = lkConfig["lk"]["apikey"]
        apikey_v2 = lkConfig["lk"]["apikeyv2"]
        if method == "ordersv2":
            cli = wb_client.WBApiClient(wb_id, apikey_v2)
            datefrom, dateto = fill_date(
                option, bqtable, datasetid, jsonkey, wb_id, field_date
            )
            logger.info(
                f"Начало импорта {method} из WB {wb_id} c {datefrom} по {dateto}:"
            )
            orders = cli.get_orders_v2(datefrom, dateto)
            localTimeDelta = datetime.timedelta(hours=3, minutes=0)
            field_date = "dateCreatedLocal"

            for index, el in enumerate(orders):
                wb_client.addSharedField(el, wb_id)
                el[field_date] = (
                    datetime.datetime.strptime(
                        el["dateCreated"], "%Y-%m-%dT%H:%M:%S.%fZ"
                    )
                    + localTimeDelta
                ).isoformat()
                if (
                    el.__contains__("deliveryAddressDetails")
                    and type(el["deliveryAddressDetails"]) is dict
                ):
                    el = el | el["deliveryAddressDetails"]

                if el.__contains__("userInfo") and type(el["userInfo"]) is dict:
                    el = el | el["userInfo"]
                if el.__contains__("entrance") and (
                    el["entrance"] == "" or type(el["entrance"]) is not str
                ):
                    el["entrance"] = str(el["entrance"])
                    if el["entrance"] == "":
                        el["entrance"] = " "
                del el["deliveryAddressDetails"]
                del el["userInfo"]
                orders[index] = el

            if len(orders) > 0:

                clean_table_if_necessary(
                    datasetid,
                    datefrom,
                    dateto,
                    field_date,
                    jsonkey,
                    logger,
                    method,
                    bqtable,
                    wb_id,
                    option,
                )
                logger.info(f"Загружаем записи {method} из WB с {datefrom}:")
                bq_method.export_js_to_bq(
                    orders, bqtable, jsonkey, datasetid, logger, []
                )
            else:
                logger.info("Нет данных")
            logger.info(f"end")
        elif method == "stocks_v2":
            cli = wb_client.WBApiClient(wb_id, apikey_v2)
            if datefrom == "":
                datefrom = datetime.date.today()
                dateto = datetime.date.today()
            # datefrom, dateto = fill_date(option,tablebq,datasetid,jsonkey,wb_id,field_date,datefromstr,datetostr)
            logger.info(f"Начало импорта {method} из WB {wb_id}:")
            stocks = cli.get_stocks_v2()
            field_date = "date_stocks"
            if len(stocks) > 0:
                clean_table_if_necessary(
                    datasetid,
                    datefrom,
                    dateto,
                    field_date,
                    jsonkey,
                    logger,
                    method,
                    bqtable,
                    wb_id,
                    option,
                    stocks,
                )
                logger.info(f"Загружаем записи {method} из WB с {datefrom}:")
                bq_method.export_js_to_bq(
                    stocks, bqtable, jsonkey, datasetid, logger, []
                )
            else:
                logger.info("Нет данных")
            logger.info(f"end")
        elif method in ("sales", "reportsale", "orders", "stocks_v1"):
            cli = wb_client.WBApiClient(wb_id, key_v1=apikey_v1, key_v2=apikey_v2)
            period = False
            if method == "stocks_v1":
                if datefrom == "":
                    datefrom = datetime.date.today()
                    dateto = datefrom
                    period = False
                else:
                    period = True

            else:
                datefrom, dateto = fill_date(
                    option,
                    bqtable,
                    datasetid,
                    jsonkey,
                    wb_id,
                    field_date,
                    method,
                    datefromstr,
                    datetostr,
                )
            logger.info(
                f"Начало импорта {method} из WB {wb_id} c {datefrom} по {dateto}:"
            )
            if method == "sales":
                orders = cli.get_sales_v1(datefrom, dateto, option, field_date)
            elif method == "reportsale":
                field_date = "rr_dt"
                orders = cli.get_reportsale_v1(datefrom, dateto, option, field_date)
                # определим дату минимальную из полученных данных, для очистки в  bq
                datefrom = parser.parse(
                    min(orders, key=lambda x: x[field_date])[field_date]
                ).replace(tzinfo=None)
            elif method == "orders":
                orders = cli.get_orders_v1(datefrom, dateto, option, field_date)
            elif method == "stocks_v1":
                field_date = "date_stocks"
                orders = cli.get_stocks_v1()

            if len(orders) > 0:
                clean_table_if_necessary(
                    datasetid,
                    datefrom,
                    dateto,
                    field_date,
                    jsonkey,
                    logger,
                    method,
                    bqtable,
                    wb_id,
                    option,
                    orders,
                )
                logger.info(f"Загружаем записи {method} из WB с {datefrom}:")
                if period == True:
                    for datestock in daterange(datefrom, dateto):
                        for el in orders:
                            el["date_stocks"] = datestock.isoformat()
                        bq_method.export_js_to_bq(
                            orders, bqtable, jsonkey, datasetid, logger, []
                        )
                else:
                    bq_method.export_js_to_bq(
                        orders, bqtable, jsonkey, datasetid, logger, []
                    )
            else:
                logger.info("Нет данных")
            logger.info(f"end")


def transfer_orders_transaction_ozon2bq_in_the_period(
    daterange,
    bqdataset,
    bqjsonservicefile,
    bqtable,
    configyml,
    fieldname,
    method,
    ozon_data_filter_type,
):
    apimethods = ozon_method.apimethods
    with open(configyml) as f:
        config = yaml.safe_load(f)
    for lkConfig in config["lks"]:
        ozonid = lkConfig["lk"]["bq_id"]
        apikey = lkConfig["lk"]["apikey"]
        clientid = lkConfig["lk"]["clientid"]

        logger.info(f"Начало импорта из OZON {ozonid}:")
        datefrom = daterange["datefrom"]
        dateto = daterange["dateto"]

        try:
            #   js=ozon_method.ozon_import(apimethods.get(method),apikey,LOG_FILE,dateimport,maxdatechange)
            # clientid='44346'

            items = ozon_method.ozon_import(
                method,
                apimethods.get(method),
                apikey,
                clientid,
                ozonid,
                datefrom,
                dateto,
                ozon_data_filter_type,
            )
            if len(items) != 0:
                logger.info(f"Чистим  данные в {bqtable} c {datefrom} по {dateto}")
                pattern_dateto = "%Y-%m-%d"
                if fieldname == "created_at":
                    pattern_dateto = "%Y-%m-%d 23:59:59"

                filterList = []
                filterList.append(
                    {
                        "fieldname": "ozon_id",
                        "operator": "=",
                        "value": ozonid,
                    }
                )

                filterList.append(
                    {
                        "fieldname": fieldname,
                        "operator": ">=",
                        "value": datefrom.strftime("%Y-%m-%d"),
                    }
                )
                filterList.append(
                    {
                        "fieldname": fieldname,
                        "operator": "<=",
                        "value": dateto.strftime(pattern_dateto),
                    }
                )
                bq_method.DeleteRowFromTable(
                    bqtable, bqdataset, bqjsonservicefile, filterList
                )
                fields_list = ozon_method.fields_from_method(method)
                bq_method.export_js_to_bq(
                    items, bqtable, bqjsonservicefile, bqdataset, logger, fields_list
                )
            else:
                logger.info(f"Данных нет {method} c {datefrom} по {dateto}")
        except Exception as e:
            logger.exception("Ошибка выполнения." + e.__str__())


def export_stocks_from_ozon2bq(bqdataset, bqjsonservicefile, bqtable, configyml):
    method = "stocks_v3"
    with open(configyml) as f:
        config = yaml.safe_load(f)

    for lkConfig in config["lks"]:
        ozonid = lkConfig["lk"]["bq_id"]
        apikey = lkConfig["lk"]["apikey"]
        clientid = lkConfig["lk"]["clientid"]
        cli = OZONApiClient(clientid, apikey, ozonid)
        logger.info(f"Начало импорта из OZON {ozonid}:")
        items = cli.get_stocks_v2()
        datetime.date.today().isoformat()
        if len(items) != 0:
            datestocks = items[0]["date"]
            logger.info(f"Чистим  данные в {bqtable} c {datestocks} ")
            fieldname = "date"
            filterList = []
            filterList.append(
                {
                    "fieldname": fieldname,
                    "operator": ">=",
                    "value": datestocks,
                }
            )
            bq_method.DeleteRowFromTable(
                bqtable, bqdataset, bqjsonservicefile, filterList
            )
            fields_list = ozon_method.fields_from_method(method)
            bq_method.export_js_to_bq(
                items, bqtable, bqjsonservicefile, bqdataset, logger, fields_list
            )
            text = f"Всё выгружено {method} за {datestocks} "
        else:
            text = f"Данных нет {method} c {datestocks}  - {ozonid}"
            logger.info(text)
