import datetime
import time

import requests
from dateutil import parser
from loguru import logger


def get_statistic_url():
    return "https://statistics-api.wildberries.ru/"


class WBApiClient:
    def __init__(self, wbid, api_key,proxi=''):
        self.api_key = api_key
        self.wbid = wbid
        self.proxi=proxi

    def get_header_v2(self):
        return {"Authorization": self.api_key, "Content-Type": "application/json"}

    def get_orders_v2(self, datefrom, dateto):
        url = "https://suppliers-api.wildberries.ru/api/v2/orders"
        skipping = 0

        take = 1000
        filters = {
            "date_start": datefrom.strftime("%Y-%m-%dT00:00:00+03:00"),
            "date_end": dateto.strftime("%Y-%m-%dT23:59:59+03:00"),
            "take": take,
            "skip": skipping,
        }
        orderstotal = self.get_js_request_v2(filters, skipping, take, url, "orders")

        return orderstotal

    def get_stocks_v2(self):
        url = "https://suppliers-api.wildberries.ru/api/v2/stocks"
        skipping = 0
        take = 1000
        filters = {"take": take, "skip": skipping}
        js_result = self.get_js_request_v2(filters, skipping, take, url, "stocks")
        for element in js_result:
            element["date_stocks"] = datetime.date.today().isoformat()
            addSharedField(element, self.wbid)
            if element.__contains__("size"):
                element["size"] = str(element["size"])
            if element.__contains__("barcode"):
                element["size"] = str(element["barcode"])
            if element.__contains__("barcodes"):
                del element["barcodes"]

        return js_result

    def get_js_request_v2(self, filters, skipping, take, url, data_block):
        headers = self.get_header_v2()
        proxies = None
        if self.proxi != '':
            proxies = {'http': self.proxi, 'https': self.proxi}
        orderstotal = []
        while True:
            res = requests.get(url, params=filters, headers=headers,proxies=proxies)
            resjson = res.json()
            orderstotal = orderstotal + resjson[data_block]
            if len(resjson[data_block]) == take:
                skipping = skipping + take
                filters["skip"] = skipping
                continue

            break
        return orderstotal

    def get_orders_v1(self, datefrom, dateto, option, filterfielddate):
        uri = get_statistic_url() + "/api/v1/supplier/orders"
        flag = 1
        jsresult = []
        if option == "changes":
            flag = 0
        params = [
            ("dateFrom", datefrom.isoformat()),
            ("dateto", dateto.isoformat()),
            #    ("key", self.api_key),
            ("flag", flag),
        ]
        res = self._make_request_v1(uri, params)
        if res != None:
            jsresult = transform_res2js(
                filterfielddate, res, datefrom, self.wbid, option, dateto
            )
        return jsresult

    def get_invoice_v1(self, datefrom, dateto, option, filterfielddate):
        uri = get_statistic_url() + "/api/v1/supplier/incomes"
        jsresult = []
        params = [
            ("dateFrom", datefrom.isoformat()),
            ("dateto", dateto.isoformat()),
            #  ("key", self.api_key),
        ]
        res = self._make_request_v1(uri, params)
        if res != None:
            jsresult = transform_res2js(
                filterfielddate, res, datefrom, self.wbid, option, dateto
            )

            for el in jsresult:
                el["date"] = el["date"][0:10]
                el["dateClose"] = el["dateClose"][0:10]
                el["date_accepted"] = None
                el["date_acceptance"] = None
                el["date_warehousecheck"] = None
                el["date_financecheck"] = None
                if el["status"] == "Принято":
                    el["date_accepted"] = el["lastChangeDate"]
                elif el["status"] == "Приемка":
                    el["date_acceptance"] = el["lastChangeDate"]
                elif el["status"] == "Проверено складом":
                    el["date_warehousecheck"] = el["lastChangeDate"]
                elif el["status"] == "В разработке фин отделом":
                    el["date_financecheck"] = el["lastChangeDate"]

        return jsresult

    def get_stocks_v1(self):
        url = get_statistic_url() + "/api/v1/supplier/stocks"
        filterfielddate = "date_stocks"

        params = [
            ("dateFrom", datetime.datetime(2020, 1, 1).isoformat()),
            # ("key", self.api_key),
        ]
        res = self._make_request_v1(url, params)
        if res != None:
            jsres = res.json()
            if len(jsres) == 0:
                logger.info(f"Ошибка запроса. Статус ответа:{res.status_code}")
                pass

            for element in jsres:
                element["date_stocks"] = datetime.date.today().isoformat()
                addSharedField(element, self.wbid)
                if element.__contains__("size"):
                    element["size"] = str(element["size"])
                if element.__contains__("techSize"):
                    element["techSize"] = str(element["techSize"])
                    if element["techSize"] == "0":
                        element["techSize"] = " "
                if element.__contains__("barcode"):
                    element["size"] = str(element["barcode"])
                if element.__contains__("barcodes"):
                    del element["barcodes"]

            return jsres

    def get_sales_v1(self, datefrom, dateto, option, filterfielddate):
        uri = get_statistic_url() + "/api/v1/supplier/sales"
        flag = 1
        filterfielddate = "date"
        if option == "changes":
            flag = 0
            filterfielddate = "lastChangeDate"
        params = [
            ("dateFrom", datefrom.isoformat()),
            ("dateto", dateto.isoformat()),
            # ("key", self.api_key),
            ("flag", flag),
        ]
        res = self._make_request_v1(uri, params)
        jsresult = transform_res2js(
            filterfielddate, res, datefrom, self.wbid, option, dateto
        )
        return jsresult

    def get_reportsale_v1(self, datefrom, dateto, option, filterfielddate):
        uri = get_statistic_url() + "/api/v1/supplier/reportDetailByPeriod"
        rrdid = 0
        jsresulttotal = []
        jsresult = self.get_result_from_query(
            datefrom, dateto, filterfielddate, option, rrdid, uri
        )

        for count_exequte in range(100):
            if len(jsresult) > 0:
                jsresulttotal = jsresulttotal + jsresult
                rrdid = jsresult[-1]["rrd_id"]
                jsresult = self.get_result_from_query(
                    datefrom, dateto, filterfielddate, option, rrdid, uri
                )
            else:
                break
        return jsresulttotal

    def get_result_from_query(
        self, datefrom, dateto, filterfielddate, option, rrdid, uri
    ):
        params = [
            ("dateFrom", datefrom.isoformat()),
            ("dateto", dateto.isoformat()),
            # ("key", self.api_key),
            ("rrdid", rrdid),
        ]
        res = self._make_request_v1(uri, params)
        jsresult = transform_res2js(
            filterfielddate, res, datefrom, self.wbid, option, dateto
        )
        return jsresult

    def _make_request_v1(self, uri, params, timeout=60):
        headers = self.get_header_v2()
        proxies = None
        if self.proxi != '':
            proxies = {'http': self.proxi, 'https': self.proxi}
        for i in range(1, 5):
            try:
                res = requests.get(uri, params=params, timeout=timeout, headers=headers,proxies=proxies)
                if res.status_code == 200:
                    return res
                if res.status_code == 429:
                    if res.text.find("перерыв") > -1:
                        raise f"Перерыв работы сервиса WB:{uri}"
                    logger.info(f"Too many requests, wait 60 sec:{uri}")
                    time.sleep(timeout)
                    continue
                elif res.status_code == 502:
                    logger.info(f"502 error, wait 60 sec:{uri}")
                    time.sleep(timeout)
                    continue
                else:
                    logger.info(f"Ошибка запроса. Статус ответа:{res.status_code}")
                    break
            except requests.exceptions.ReadTimeout:
                timeout = +60
                continue
            except Exception as e:
                raise e
        logger.error(f"Ошибка запроса:{uri}")
        logger.error(f"параметры:{params}")

        return


def transform_res2js(filter_field_date, res, datefrom, wb_id, option, dateto):
    if res == None:
        return []
    jsres = res.json()
    if jsres == None or len(jsres) == 0:
        # logger.info(f"Ошибка запроса или нет данных. Статус ответа:{res.status_code}")
        return []
    if filter_field_date != "":
        if option == "changes":
            jsresult = list(
                filter(
                    lambda x: parser.parse(x[filter_field_date]).replace(tzinfo=None)
                    > datefrom
                    and parser.parse(x[filter_field_date]).replace(tzinfo=None)
                    <= dateto,
                    jsres,
                )
            )
        else:
            jsresult = list(
                filter(
                    lambda x: parser.parse(x[filter_field_date]).replace(tzinfo=None)
                    >= datefrom
                    and parser.parse(x[filter_field_date]).replace(tzinfo=None)
                    <= dateto,
                    jsres,
                )
            )

        if len(jsresult) == 0:
            logger.info(
                f"Нет новых данных c {datefrom}. Статус ответа:{res.status_code}"
            )
            pass
    else:
        jsresult = jsres
    float_fields = (
        "customer_reward",
        "finishedPrice",
        "sale_percent",
        "priceWithDisc",
        "totalPrice",
        "ppvz_reward",
        "retail_price_withdisc_rub",
    )
    int_fields = ("barcode","techSize", "ts_name", "ppvz_inn", "penalty", "additional_payment")
    str_fields = ("gNumber", "sticker")
    for el in jsresult:
        if "number" in el:
            del el["number"]
        addSharedField(el, wb_id)
        for key in el.keys():
            if key in float_fields:
                el[key] = parse_float(el[key])
            elif key in int_fields:
                el[key] = parse_int(el[key])
            elif key in str_fields:
                el[key] = str(el[key])


    return jsresult


def addSharedField(el, wb_id):
    el["wb_id"] = wb_id
    el["dateExport"] = datetime.datetime.today().isoformat()


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
