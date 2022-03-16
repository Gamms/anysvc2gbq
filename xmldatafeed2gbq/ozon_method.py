import datetime
import json
import time
from enum import Enum

import requests
from common_type import Struct
from dateutil import parser
from loguru import logger

timeout = 60  # таймаут 60 секунд
apimethods = {
    "transaction": "https://api-seller.ozon.ru/v2/finance/transaction/list",
    "transactionv3": "https://api-seller.ozon.ru/v3/finance/transaction/list",
    "stock": "https://api-seller.ozon.ru/v1/product/info/stocks",
    "orders": "https://api-seller.ozon.ru/v2/posting/fbs/list",
    "fbo_orders": "https://api-seller.ozon.ru/v2/posting/fbo/list",
    "price": "https://api-seller.ozon.ru/v1/product/info/prices",
    "orders_v3": "https://api-seller.ozon.ru/v3/posting/fbs/list",
}

data_filter_type = Struct


class OzonDataFilterType(str, Enum):
    order_created_at = "order_created_at"  # order
    in_process_at = "in_process_at"  # order
    updated_at = "updated_at"  # order
    date = "date"  # transaction
    since = "since"  # fboorders
    order_id = "order_id"


def ozon_import(
    method,
    apimethods,
    apikey,
    clientid,
    ozonid,
    datefrom,
    dateto,
    ozon_data_filter_type: OzonDataFilterType,
    order_id=0,
):
    # делаем 5 попыток с паузой 1 минута, если не вышло пропускаем

    items = query(
        apimethods,
        apikey,
        clientid,
        method,
        ozonid,
        datefrom,
        dateto,
        ozon_data_filter_type,
        order_id,
    )

    return items


def checkTypeFieldFloat(newdict, elfield):
    if newdict.__contains__(elfield) and type(newdict[elfield]) is not float:
        newdict[elfield] = parse_float(newdict[elfield])


def query(
    apiuri,
    apikey,
    clientid,
    method,
    ozon_id,
    datefrom,
    dateto,
    ozon_data_filter_type: OzonDataFilterType,
    order_id=0,
):
    page = 1
    querycount = 1000
    data, querycount = makedata(
        page, querycount, method, datefrom, dateto, ozon_data_filter_type, order_id
    )
    headers = {"Api-Key": apikey, "Client-Id": clientid}
    res = make_query("post", apiuri, data, headers)
    js = json.loads(res.text)
    # фильтруем то что уже есть
    items = datablock_from_js(js, method)
    itemstotal = items
    while len(items) == querycount:
        # количество записей видимо больше запросим следующую страниц
        page = page + 1  #
        data, querycount = makedata(
            page, querycount, method, datefrom, dateto, ozon_data_filter_type, order_id
        )
        res = make_query("post", apiuri, data, headers)
        js = json.loads(res.text)
        items = datablock_from_js(js, method)
        # дополним последующими записями
        itemstotal = itemstotal + items
    if method == "orders" or method == "fbo_orders":
        # apiurlproduct = 'https://api-seller.ozon.ru/v1/product/list'#озон не отдает артикулы сразу, нужен доп запрос
        # resproduct = make_query('post', apiurlproduct, data, headers, logers)
        # jsproduct = json.loads(resproduct.text)
        # itemsproduct=jsproduct['result']['items']
        # products = {item['product_id']: item['offer_id'] for item in itemsproduct}
        newlist = []
        for el in itemstotal:
            if (
                el.__contains__("financial_data") and type(el["financial_data"]) is dict
            ):  # проверим наличие финансового блока
                for element_product_financial in el["financial_data"][
                    "products"
                ]:  # пробежимся по тч товаров из финансового блока
                    postingservice = el["financial_data"]["posting_services"]
                    analiticsdata = el["analytics_data"]
                    newdict = el | element_product_financial | postingservice
                    if not postingservice is None:
                        newdict = newdict | postingservice
                    if not analiticsdata is None:

                        newdict = newdict | analiticsdata

                    if el.__contains__("barcodes") and type(el["barcodes"]) is dict:
                        newdict = newdict | el["barcodes"]
                    newdict["ozon_id"] = ozon_id
                    newdict["dateExport"] = datetime.datetime.today().isoformat()
                    if method == "orders":
                        for elfield in [
                            "commission_amount",
                            "payout",
                            "total_discount_value",
                            "old_price",
                            "marketplace_service_item_deliv_to_customer",
                        ]:
                            checkTypeFieldFloat(newdict, elfield)
                    if method == "fbo_orders":
                        for elfield in [
                            "total_discount_value",
                            "old_price",
                            "payout",
                            "commission_amount",
                            "total_discount_percent",
                        ]:
                            checkTypeFieldFloat(newdict, elfield)

                    for product in el["products"]:
                        if product["sku"] == newdict["product_id"]:
                            newdict["offer_id"] = product["offer_id"]
                            newdict["offer_name"] = product["name"]
                            break

                    for key, value in list(newdict.items()):  # удалим ненужные элементы
                        if (
                            type(value) is list
                            or type(value) is dict
                            or key == "analytics_data"
                        ):
                            del newdict[key]

                    newlist.append(newdict)

        itemstotal = newlist
    elif method == "transactionv3":
        itemstotal = js_2_plainjs(itemstotal, method, ozon_id)
    else:
        for el in itemstotal:

            for elfield in ["order_amount", "commission_amount"]:
                checkTypeFieldFloat(el, elfield)
            el["ozon_id"] = ozon_id
            el["dateExport"] = datetime.datetime.today().isoformat()
            if method == "price":
                if el["price"]["recommended_price"] == "":
                    el["price"]["recommended_price"] = 0.0
            # if method == 'transaction':
            # if type(el['order_amount']) is float:
            #    print(el['orderAmount'])
            #    el['orderAmount']=int(el['orderAmount'])

    return itemstotal


def js_2_plainjs(js, method, ozon_id):
    if method == "transactionv3":
        newlist = []
        for el in js:
            sumservices = 0
            servicestr = ""
            for service in el["services"]:
                servicestr = f'{servicestr}{service["name"]}:{service["price"]}'
                sumservices = sumservices + service["price"]

            for item in el["items"]:
                newdict = el | item | el["posting"]
                newdict["services_list"] = servicestr
                newdict["services_price_total"] = sumservices
                newdict["ozon_id"] = ozon_id

                newdict["operation_date"] = strdate_to_isodate(
                    newdict["operation_date"]
                )
                newdict["order_date"] = strdate_to_isodate(newdict["order_date"])
                newdict["dateExport"] = datetime.datetime.today().isoformat()
                del newdict["posting"]
                del newdict["items"]
                del newdict["services"]

                newlist.append(newdict)
    else:
        newlist = js
    return newlist


def strdate_to_isodate(strdate):
    if strdate != "":
        date = parser.parse(strdate)
        if type(date) == datetime.date:
            result = date.isoformat()
        else:
            result = date.date().isoformat()
    else:
        result = datetime.date(1, 1, 1).isoformat()
    return result


def datablock_from_js(js, method):
    if method == "stock" or method == "price":
        items = js["result"]["items"]
    if method == "transactionv3":
        items = js["result"]["operations"]
    else:
        items = js["result"]
    return items


def makedata(
    page,
    querycount,
    method,
    datefrom,
    dateto,
    ozon_data_filter_type: OzonDataFilterType,
    order_id=0,
):
    datefromstr = datefrom.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    datetostr = dateto.strftime("%Y-%m-%dT23:59:59.000Z")
    if method == "stock" or method == "price":
        data = f'{{"page": {page},"page_size": {querycount}}}'
    elif ozon_data_filter_type == OzonDataFilterType.date:
        # data =  f'{{"filter": {{"date": {{"from": "2020-01-01T00:00:00.999Z","to": "2020-12-31T23:59:59.999Z"}},'\
        data = (
            f'{{"filter": {{"date": {{"from": "{datefromstr}","to": "{datetostr}"}},'
            f'"transaction_type": "all"}}'
            f',"page": {page},"page_size": {querycount}}}'
        )
    elif ozon_data_filter_type == OzonDataFilterType.since:  # fbo orders
        querycount = 1000
        ofset = (page - 1) * querycount
        data = f'{{"dir": "asc","filter": {{"since": "{datefromstr}","to": "{datetostr}"}},"offset": {ofset},"limit": {querycount},"with": {{"barcodes":true,"financial_data": true,"analytics_data": true}}}}'
    elif ozon_data_filter_type == OzonDataFilterType.order_id:
        querycount = 1000
        ofset = (page - 1) * querycount
        data = f'{{"dir": "asc","filter": {{"order_id": {order_id},"offset": {ofset},"limit": {querycount},"with": {{"barcodes":true,"financial_data": true,"analytics_data": true}}}}'

    else:  # orders created_at
        querycount = 1000
        ofset = (page - 1) * querycount
        data = f'{{"dir": "asc","filter": {{"{ozon_data_filter_type.name}":{{"from": "{datefromstr}","to": "{datetostr}"}}}},"offset": {ofset},"limit": {querycount},"with": {{"barcodes":true,"financial_data": true,"analytics_data": true}}}}'

    return data, querycount


def make_query(method, uri, data, headers):
    result = 0
    for i in range(1, 5):

        if method == "post":
            res = requests.post(uri, data=data, headers=headers)
        else:
            res = requests.get(uri, data=data, headers=headers)

        if res.status_code == 429:
            logger.info(f"Too many requests, wait 60 sec:{uri}")
            time.sleep(timeout)
        if res.status_code == 408:
            logger.info(f"Timeout code 408, wait 5 sec:{uri}")
            time.sleep(5)

        elif res.status_code != 200:
            logger.info(f"Ошибка запроса. Статус ответа:{res.status_code}")
            break
        else:
            result = res
            break
    return result


def parse_int(s):
    try:
        res = int(eval(str(s)))
        if type(res) == int:
            return res
    except:
        return 0


def parse_float(s):
    try:
        res = float(eval(str(s)))
        if type(res) == float:
            return res
    except:
        return 0.0


def fields_from_method(method):
    if method == "transactionv3":
        csvfields = []
        csvfields.append({"operation_id": "STRING"})
        csvfields.append({"operation_type": "STRING"})
        csvfields.append({"operation_date": "DATE"})
        csvfields.append({"operation_type_name": "STRING"})
        csvfields.append({"delivery_charge": "FLOAT"})
        csvfields.append({"return_delivery_charge": "FLOAT"})
        csvfields.append({"accruals_for_sale": "FLOAT"})
        csvfields.append({"sale_commission": "FLOAT"})
        csvfields.append({"amount": "FLOAT"})
        csvfields.append({"type": "STRING"})
        csvfields.append({"name": "STRING"})
        csvfields.append({"sku": "STRING"})
        csvfields.append({"delivery_schema": "STRING"})
        csvfields.append({"order_date": "DATE"})
        csvfields.append({"posting_number": "STRING"})
        csvfields.append({"warehouse_id": "STRING"})
        csvfields.append({"services_list": "STRING"})
        csvfields.append({"services_price_total": "FLOAT"})
        csvfields.append({"ozon_id": "STRING"})
        csvfields.append({"dateExport": "TIMESTAMP"})
    elif method == "___orders" or method == "___fbo_orders":
        csvfields = []
        csvfields.append({"payment_type_group_name": "STRING"})
        csvfields.append({"delivery_type": "STRING"})
        csvfields.append({"city": "STRING"})
        csvfields.append({"lower_barcode": "INTEGER"})
        csvfields.append({"upper_barcode": "STRING"})
        csvfields.append(
            {"marketplace_service_item_return_after_deliv_to_customer": "INTEGER"}
        )
        csvfields.append(
            {"marketplace_service_item_return_part_goods_customer": "INTEGER"}
        )
        csvfields.append(
            {"marketplace_service_item_return_not_deliv_to_customer": "INTEGER"}
        )
        csvfields.append({"marketplace_service_item_direct_flow_trans": "INTEGER"})
        csvfields.append({"marketplace_service_item_dropoff_ff": "INTEGER"})
        csvfields.append({"marketplace_service_item_deliv_to_customer": "INTEGER"})
        csvfields.append({"marketplace_service_item_pickup": "INTEGER"})
        csvfields.append({"client_price": "STRING"})
        csvfields.append({"quantity": "INTEGER"})
        csvfields.append({"marketplace_service_item_return_flow_trans": "INTEGER"})
        csvfields.append({"marketplace_service_item_dropoff_pvz": "INTEGER"})
        csvfields.append({"picking": "STRING"})
        csvfields.append({"is_premium": "BOOL"})
        csvfields.append({"marketplace_service_item_dropoff_sc": "INTEGER"})
        csvfields.append({"shipment_date": "TIMESTAMP"})
        csvfields.append({"total_discount_percent": "FLOAT"})
        csvfields.append({"old_price": "FLOAT"})
        csvfields.append({"price": "INTEGER"})
        csvfields.append({"marketplace_service_item_fulfillment": "INTEGER"})
        csvfields.append({"product_id": "INTEGER"})
        csvfields.append({"region": "STRING"})
        csvfields.append({"order_number": "STRING"})
        csvfields.append({"commission_percent": "INTEGER"})
        csvfields.append({"payout": "FLOAT"})
        csvfields.append({"in_process_at": "TIMESTAMP"})
        csvfields.append({"created_at": "TIMESTAMP"})
        csvfields.append({"order_id": "TIMESTAMP"})
        csvfields.append({"posting_number": "STRING"})
        csvfields.append({"cancel_reason_id": "INTEGER"})
        csvfields.append({"commission_amount": "FLOAT"})
        csvfields.append({"status": "STRING"})
        csvfields.append({"offer_id": "STRING"})
        csvfields.append({"offer_name": "STRING"})

    else:
        csvfields = []
    return csvfields
