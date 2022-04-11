import datetime

import ozon_method


class OZONApiClient:
    ENDPOINT: str = "https://api-seller.ozon.ru"

    def __init__(self, client_id, api_key, ozon_id):
        self.client_id = client_id
        self.api_key = api_key
        self.ozon_id = ozon_id
        self.date_export = datetime.datetime.now()

    def get_header(self):
        headers = {"Api-Key": self.api_key, "Client-Id": self.client_id}
        return headers

    def get_stocks_v2(self) -> dict:
        methoduri = "/v3/product/info/stocks"
        uri = self.ENDPOINT + methoduri
        last_id = ""
        limit = 1000
        itemstotal = []
        while True:
            data_json = self.get_datafilter_stock(limit, last_id)
            result = ozon_method.make_query("post", uri, "", self.get_header(), data_json=data_json)
            js = result.json()
            items_temp = ozon_method.datablock_from_js(js, "stock")
            items = []
            for elem in items_temp:
                for elem_stock in elem["stocks"]:
                    newdict = {"product_id": elem["product_id"], "offer_id": elem["offer_id"],
                               "type": elem_stock["type"], "present": elem_stock["present"],
                               "reserved": elem_stock["reserved"], "ozon_id": self.ozon_id,
                               "date": self.date_export.date().isoformat(), "dateExport": self.date_export.isoformat()}

                    items.append(newdict)

            itemstotal = itemstotal + items
            if len(items_temp) < limit:
                break
            else:
                last_id = js["result"]["last_id"]

        return itemstotal

    def get_orders_v3(self,datefromstr,datetostr) -> dict:
        methoduri = "/v3/posting/fbs/list"
        uri = self.ENDPOINT + methoduri
        itemstotal = []
        page: int=1
        while True:
            data_json = self.get_datafilter_orders(datefromstr,datetostr,page)
            result = ozon_method.make_query("post", uri, "", self.get_header(), data_json=data_json)
            js = result.json()
            result=js['result']
            itemstotal = itemstotal + result['postings']
            if not result['has_next']:
                break
            page=page+1

        return itemstotal

    @staticmethod
    def get_datafilter_stock(limit, last_id):
        data = {"filter": {}, "limit": limit, "last_id": last_id}
        return data

    @staticmethod
    def get_datafilter_orders(datefromstr, datetostr, page,
                              limit=1000, financial_data=False, analytics_data=False, barcodes=False):
        ofset = (page - 1) * limit
        data = {'dir': 'asc',
                "filter": {'since':datefromstr,'to':datetostr},
                "offset": ofset,
                "limit": limit,
                "with": {'financial_data': financial_data,'analytics_data': analytics_data,'barcodes': barcodes}
                }
        return data
