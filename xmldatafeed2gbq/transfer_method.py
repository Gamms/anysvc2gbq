import datetime

import bq_method
import ozon_method
import wb_client
import yaml
import yandex.yclient
from client1c import Client1c, daterange
from dateutil import parser
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
            if method == "orders":
                add_fields_from_orders_v3(apikey, clientid, items, ozonid)

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


def add_fields_from_orders_v3(apikey, clientid, items, ozonid):
    cli = OZONApiClient(clientid, apikey, ozonid)
    min_date = min(items, key=lambda x: x["created_at"])["created_at"]
    max_date = max(items, key=lambda x: x["created_at"])["created_at"]
    logger.info(
        f"дополним данные заказа v3 из OZON {ozonid} c {min_date} по {max_date}:"
    )
    orders_v3 = cli.get_orders_v3(min_date, max_date)
    for element_items in items:
        filterlist = list(
            filter(lambda x: x["order_id"] == element_items["order_id"], orders_v3)
        )
        if len(filterlist):
            element_items["is_express"] = filterlist[0]["is_express"]
            element_items["delivery_method_name"] = filterlist[0]["delivery_method"][
                "name"
            ]
            element_items["delivery_method_warehouse"] = filterlist[0][
                "delivery_method"
            ]["warehouse"]
            element_items["delivery_method_tpl_provider"] = filterlist[0][
                "delivery_method"
            ]["tpl_provider"]
            element_items["tpl_integration_type"] = filterlist[0][
                "tpl_integration_type"
            ]
        else:
            element_items["is_express"] = False
            element_items["delivery_method_name"] = ""
            element_items["delivery_method_warehouse"] = ""
            element_items["delivery_method_tpl_provider"] = ""
            element_items["tpl_integration_type"] = ""


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
                if method == "orders":
                    add_fields_from_orders_v3(apikey, clientid, items, ozonid)

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
        if not lkConfig["lk"]["active"]:
            logger.info(
                f"Импорт из WB {wb_id} отключен в настройках (свойство active из yml)"
            )

            continue

        if method == "ordersv2":
            cli = wb_client.WBApiClient(wb_id, apikey_v2)
            datefrom, dateto = fill_date(
                option,
                bqtable,
                datasetid,
                jsonkey,
                wb_id,
                field_date,
                "",
                datefromstr,
                datetostr,
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
                checkTypeFieldFloat(el, "latitude")
                checkTypeFieldFloat(el, "longitude")
                checkTypeFieldFloat(el, "totalPrice")
                el["rid"] = ozon_method.parse_int(el["rid"])

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
                    orders,
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
        elif method in ("sales", "reportsale", "orders", "stocks_v1", "invoice_v1"):
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
            field_list = []
            idfield = "odid"
            if method == "sales":
                orders = cli.get_sales_v1(datefrom, dateto, option, field_date)
            elif method == "reportsale":
                field_date = "rr_dt"
                orders = cli.get_reportsale_v1(datefrom, dateto, option, field_date)
                # определим дату минимальную из полученных данных, для очистки в  bq
                if len(orders) == 0:
                    logger.info(
                        f"Нет данных для {wb_id} метод {method} период с {datefrom} по {dateto}"
                    )
                    continue
                datefrom = parser.parse(
                    min(orders, key=lambda x: x[field_date])[field_date]
                ).replace(tzinfo=None)
            elif method == "orders":
                orders = cli.get_orders_v1(datefrom, dateto, option, field_date)
            elif method == "stocks_v1":
                field_date = "date_stocks"
                orders = cli.get_stocks_v1()
            elif method == "invoice_v1":
                idfield = "incomeId"
                field_date = "lastChangeDate"
                field_list.append({"wb_id": "STRING"})
                field_list.append({"dateExport": "TIMESTAMP"})
                field_list.append({"incomeid": "INTEGER"})
                field_list.append({"Number": "STRING"})
                field_list.append({"Date": "DATE"})
                field_list.append({"lastChangeDate": "TIMESTAMP"})
                field_list.append({"SupplierArticle": "STRING"})
                field_list.append({"TechSize": "STRING"})
                field_list.append({"Barcode": "STRING"})
                field_list.append({"Quantity": "INTEGER"})
                field_list.append({"totalPrice": "FLOAT"})
                field_list.append({"dateClose": "DATE"})
                field_list.append({"warehouseName": "STRING"})
                field_list.append({"nmid": "INTEGER"})
                field_list.append({"status": "STRING"})
                field_list.append({"date_accepted": "TIMESTAMP"})
                field_list.append({"date_acceptance": "TIMESTAMP"})
                field_list.append({"date_warehousecheck": "TIMESTAMP"})
                field_list.append({"date_financecheck": "TIMESTAMP"})

                orders = cli.get_invoice_v1(datefrom, dateto, option, field_date)

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
                    idfield,
                )
                logger.info(f"Загружаем записи {method} из WB с {datefrom}:")
                if period == True:
                    for datestock in daterange(datefrom, dateto):
                        for el in orders:
                            el["date_stocks"] = datestock.isoformat()
                        bq_method.export_js_to_bq(
                            orders, bqtable, jsonkey, datasetid, logger, field_list
                        )
                else:
                    bq_method.export_js_to_bq(
                        orders, bqtable, jsonkey, datasetid, logger, field_list
                    )
            else:
                logger.info("Нет данных")
            logger.info(f"end")
        else:
            raise f"Неподдерживаемый метод выгрузки из wb:{method}"


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


def export_orders_from_ym2bq(
    bqdataset="YM",
    bqjsonservicefile="polar.json",
    bqtable="orders",
    configyml="config_yandex.yml",
    dateFrom=None,
    dateTo=None,
):
    with open(configyml) as f:
        config = yaml.safe_load(f)

    for lkConfig in config["lks"]:
        campaign = lkConfig["lk"]["campaign"]
        oath_id = lkConfig["lk"]["oath_id"]
        oath_token = lkConfig["lk"]["oath_token"]
        field_date = "statusUpdateDate"
        field_id = "ycampaignid"
        if not lkConfig["lk"]["active"]:
            logger.info(
                f"Импорт из YM {campaign} {lkConfig['lk']['description']} отключен в настройках (свойство active из yml)"
            )

            continue

        newlist = []
        changes = True
        if dateFrom != None:
            changes = False
        else:
            maxdatechange = bq_method.GetMaxRecord_v1(
                bqtable,
                bqdataset,
                bqjsonservicefile,
                field_date,
                int(campaign),
                field_id,
            )

        if not changes or maxdatechange.year == 1:
            maxdatechange = datetime.date(2021, 1, 1)
            changes = (
                False  # если таблица пустая, вначале загрузим заказы с начала года
            )
        else:
            maxdatechange = maxdatechange.date()
        if dateFrom == None:
            dateFrom = maxdatechange
            dateTo = datetime.date.today()

        client = yandex.yclient.YMApiClient(campaign, oath_id, oath_token)
        itemsCatalog = client.get_catalog()
        if itemsCatalog == None:
            continue
        catalogCache = {}
        jsonCatalog = []
        for item in itemsCatalog:
            offer = item["offer"]
            if offer.__contains__("vendorCode"):
                catalogCache[offer["shopSku"]] = offer["vendorCode"]
                jsonCatalog.append(
                    {"shopSku": offer["shopSku"], "vendorCode": offer["vendorCode"]}
                )

        itemstotal = client.get_orders(dateFrom, dateTo, changes)

        for el in itemstotal:
            if (
                el.__contains__("items") and type(el["items"]) is list
            ):  # проверим наличие финансового блока
                sumCommision = 0
                for commEl in el["commissions"]:
                    sumCommision = sumCommision + commEl["predicted"]

                for item in el[
                    "items"
                ]:  # пробежимся по тч из заказа и объединим их в строку

                    newdict = el | item
                    newdict["deliveryRegionId"] = el["deliveryRegion"]["id"]
                    newdict["deliveryRegionName"] = el["deliveryRegion"]["name"]
                    for price in item["prices"]:
                        newdict[price["type"] + "costPerItem"] = price["costPerItem"]
                        newdict[price["type"] + "total"] = price["total"]

                    for key, value in item["warehouse"].items():
                        newdict["wh" + key] = value

                    newdict["ycampaignid"] = campaign
                    newdict["dateExport"] = datetime.datetime.today().isoformat()
                    newdict["sumCommision"] = sumCommision
                    newdict["articleCustomer"] = catalogCache.get(newdict["shopSku"])
                    for key, value in list(newdict.items()):  # удалим ненужные элементы
                        if type(value) is list or type(value) is dict:
                            del newdict[key]

                    newlist.append(newdict)
        if len(itemstotal) > 0 and changes == False:
            filterList = []

            filterList.append(
                {
                    "fieldname": field_date,
                    "operator": ">=",
                    "value": dateFrom.strftime("%Y-%m-%d"),
                }
            )
            filterList.append(
                {
                    "fieldname": field_date,
                    "operator": "<=",
                    "value": dateTo.strftime("%Y-%m-%d"),
                }
            )

            filterList.append(
                {"fieldname": field_id, "operator": "=", "value": int(campaign)}
            )
            bq_method.DeleteRowFromTable(
                bqtable, bqdataset, bqjsonservicefile, filterList
            )

        bq_method.export_js_to_bq(
            newlist, bqtable, bqjsonservicefile, bqdataset, logger, []
        )


def checkTypeFieldFloat(newdict, elfield):
    if newdict.__contains__(elfield) and type(newdict[elfield]) is not float:
        newdict[elfield] = ozon_method.parse_float(newdict[elfield])


def export_stocks_from_1c2ym(config_1c, config_ym):
    with open(config_1c, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    cli = Client1c(config)
    cli.connect()

    with open(config_ym) as f:
        config = yaml.safe_load(f)

    for lkConfig in config["lks"]:
        campaign = lkConfig["lk"]["campaign"]
        oath_id = lkConfig["lk"]["oath_id"]
        oath_token = lkConfig["lk"]["oath_token"]
        id_organisation_1c = lkConfig["lk"]["id_organisation_1c"]
        id_partner_1c = lkConfig["lk"]["id_partner_1c"]
        id_warehouse = lkConfig["lk"]["id_warehouse"]
        liststock = cli.get_stocks_for_marketplace(id_organisation_1c, id_partner_1c)
        if liststock == None:
            logger.critical("Нет подключения к базе 1С!")
            continue

        client = yandex.yclient.YMApiClient(campaign, oath_id, oath_token)
        result = client.put_stocks(liststock, id_warehouse)
        if result != "Ok":
            logger.critical("Ошибка выгрузки остатков!")
        else:
            logger.info(
                f"Выгружены остатки в яндекс по организации {id_organisation_1c}, количество:{len(liststock)}"
            )
        continue
