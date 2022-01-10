import ozon_method
import bq_method
import yaml
from loguru import logger

def export_orders_from_ozon2bq_updated_in_the_period(datarange, bqdataset, bqjsonservicefile, bqtable,
                                                     configyml, method):
    apimethods = ozon_method.apimethods
    with open(configyml) as f:
        config = yaml.safe_load(f)
    for lkConfig in config['lks']:
        ozonid = lkConfig['lk']['bq_id']
        apikey = lkConfig['lk']['apikey']
        clientid = lkConfig['lk']['clientid']

        logger.info(f'Начало импорта из OZON {ozonid}:')
        datefrom = datarange['datefrom']
        dateto = datarange['dateto']

        items = ozon_method.ozon_import(method, apimethods.get(method), apikey, clientid, ozonid, datefrom, dateto,
                                        ozon_method.OzonDataFilterType.updated_at)
        if len(items) != 0:
            logger.info(f'Чистим  данные в {bqtable} по {len(items)} заказам')
            fieldname = 'operation_date'
            filterList = []
            filterList.append(
                {
                    "fieldname": "ozon_id",
                    "operator": "=",
                    "value": ozonid,
                }
            )
            orderidlist = ''
            for elitems in items:
                if orderidlist != '':
                    orderidlist = orderidlist + ','
                order_id = elitems['order_id']
                orderidlist = orderidlist + f"'{order_id}'"

            filterList.append(
                {
                    "fieldname": "order_id",
                    "operator": " IN ",
                    "value": orderidlist,
                }
            )
            bq_method.DeleteRowFromTable(bqtable, bqdataset, bqjsonservicefile, filterList)
            fields_list = ozon_method.fields_from_method(method)
            bq_method.export_js_to_bq(items, bqtable, bqjsonservicefile, bqdataset, logger, fields_list)
            text = f'Всё выгружено {method} c {datefrom} по {dateto}'
        else:
            text=f'Данных нет {method} c {datefrom} по {dateto} - {ozonid}'
            logger.info(text)


    def transfer_orders_transaction_ozon2bq_in_the_period(daterange, bqdataset, bqjsonservicefile, bqtable, configyml,
                                                          fieldname, method, ozon_data_filter_type):
        apimethods = ozon_method.apimethods
        with open(configyml) as f:
            config = yaml.safe_load(f)
        for lkConfig in config['lks']:
            ozonid = lkConfig['lk']['bq_id']
            apikey = lkConfig['lk']['apikey']
            clientid = lkConfig['lk']['clientid']

            logger.info(f'Начало импорта из OZON {ozonid}:')
            datefrom = daterange['datefrom']
            dateto = daterange['dateto']

            try:
                #   js=ozon_method.ozon_import(apimethods.get(method),apikey,LOG_FILE,dateimport,maxdatechange)
                # clientid='44346'

                items = ozon_method.ozon_import(method, apimethods.get(method), apikey, clientid, ozonid, datefrom,
                                                dateto, ozon_data_filter_type)
                if len(items) != 0:
                    logger.info(f'Чистим  данные в {bqtable} c {datefrom} по {dateto}')

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
                            "value": dateto.strftime("%Y-%m-%d"),
                        }
                    )
                    bq_method.DeleteRowFromTable(bqtable, bqdataset, bqjsonservicefile, filterList)
                    fields_list = ozon_method.fields_from_method(method)
                    bq_method.export_js_to_bq(items, bqtable, bqjsonservicefile, bqdataset, logger, fields_list)
                else:
                    logger.info(f'Данных нет {method} c {datefrom} по {dateto}')
            except Exception as e:
                logger.exception("Ошибка выполнения." + e.__str__())
