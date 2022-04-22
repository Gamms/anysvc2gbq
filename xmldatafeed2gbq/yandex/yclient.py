import requests

class   YMApiClient:
    #регистрация приложания https://oauth.yandex.ru/client/new
    #получение токена https://oauth.yandex.ru/authorize?response_type=token&client_id=<идентификатор приложения>
    def __init__(self,campaign_id,oath_clientid,oath_tocken,endpoint='https://api.partner.market.yandex.ru/v2/campaigns/'):
        self.endpoint = endpoint
        self.oath_tocken = oath_tocken
        self.oath_clientid = oath_clientid
        self.campaign_id=campaign_id

    def get_header(self):
        return {
            'Authorization': f'OAuth oauth_token={self.oath_tocken}, oauth_client_id={self.oath_clientid}',
            'Content-Type': 'application/json'
        }


    def get_orders(self,datefrom,dateto,changes):
        method="/stats/orders.json"
        if changes:
            filter={'updateFrom':datefrom.isoformat(),
            'updateTo':dateto.isoformat()}
        else:
            filter={'dateFrom':datefrom.isoformat(),
            'dateTo':dateto.isoformat()}

        uri=self.endpoint+self.campaign_id+method
        headers=self.get_header()
        data=requests.post(uri,json=filter,headers=headers)

        datajs=data.json()
        if data.status_code!=200:
            return None
        result=datajs['result']
        orders=result['orders']
        while True:
            try:
                next_page_token = result['paging']['nextPageToken']
                uri = self.endpoint + self.campaign_id + method+f'?page_token={next_page_token}'
                data=requests.post(uri,json=filter,headers=headers)
                datajs=data.json()
                result = datajs['result']
                orders =orders+ result['orders']
            except KeyError:
                return orders
        return orders

    def get_catalog(self):
        method="/offer-mapping-entries.json"
        uri=self.endpoint+self.campaign_id+method
        headers=self.get_header()
        data=requests.get(uri,headers=headers)
        datajs=data.json()
        if data.status_code!=200:
            return None
        result=datajs['result']
        offerMappingEntries=result['offerMappingEntries']
        while True:
            try:
                next_page_token = result['paging']['nextPageToken']
                uri = self.endpoint + self.campaign_id + method+f'?page_token={next_page_token}'
                data=requests.get(uri,headers=headers)
                datajs=data.json()
                result = datajs['result']
                offerMappingEntries =offerMappingEntries+ result['offerMappingEntries']
            except KeyError:
                return offerMappingEntries

        return offerMappingEntries

