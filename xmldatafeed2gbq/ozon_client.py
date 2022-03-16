import datetime

from ozon_method import datablock_from_js, make_query


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
            data_json = self.get_datafilter(limit, last_id)
            result = make_query("post", uri, "", self.get_header(), data_json=data_json)
            js = result.json()
            items_temp = datablock_from_js(js, "stock")
            items = []
            for elem in items_temp:
                for elem_stock in elem["stocks"]:
                    newdict = {}
                    newdict["product_id"] = elem["product_id"]
                    newdict["offer_id"] = elem["offer_id"]
                    newdict["type"] = elem_stock["type"]
                    newdict["present"] = elem_stock["present"]
                    newdict["reserved"] = elem_stock["reserved"]
                    newdict["ozon_id"] = self.ozon_id
                    newdict["date"] = self.date_export.date().isoformat()
                    newdict["dateExport"] = self.date_export.isoformat()

                    items.append(newdict)

            itemstotal = itemstotal + items
            if len(items_temp) < limit:
                break
            else:
                last_id = js["result"]["last_id"]

        return itemstotal

    def get_datafilter(self, limit, last_id):
        data = {}
        data["filter"] = {}
        data["limit"] = limit
        data["last_id"] = last_id
        return data
