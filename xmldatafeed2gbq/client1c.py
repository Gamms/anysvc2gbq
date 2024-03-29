import datetime
import json
from datetime import timedelta

import bq_method
import win32com.client
import yaml
from common_type import Struct
from loguru import logger
from query1C import *


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


class Client1c:
    def __init__(self, config):
        struct_config = Struct(**config)
        self.server = struct_config.server
        self.infobase = struct_config.infobase
        self.user = struct_config.user
        self.password = struct_config.password
        self.connection = None

    def connect(self):
        CONSTR = f'Srvr="{self.server}";Ref="{self.infobase}";Usr="{self.user}";Pwd="{self.password}"'
        self.connection = win32com.client.Dispatch("V83.COMConnector").Connect(CONSTR)

    def disconnect(self):
        self.connection = None

    def get_item_ref(self) -> list:
        if self.connection == None:
            raise "Нет подключения к базе 1С"
        textquery = get_query_itemref()
        query = self.connection.NewObject("Query", textquery)
        query.SetParameter(
            "Актуальность",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Актуальность"
            ),
        )
        query.SetParameter(
            "НомерТкани",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Номер ткани"
            ),
        )
        query.SetParameter(
            "Размер",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Размер"
            ),
        )
        query.SetParameter(
            "ТипТкани",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Тип ткани"
            ),
        )
        query.SetParameter(
            "Ткань",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Ткань"
            ),
        )
        query.SetParameter(
            "Форма",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Форма"
            ),
        )
        query.SetParameter(
            "Принт",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Принт/цвет"
            ),
        )
        query.SetParameter(
            "ГруппаТкани",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Группа ткани"
            ),
        )
        query.SetParameter(
            "СтатусИзделия",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Статус изделия"
            ),
        )
        query.SetParameter(
            "ТипТовара",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Тип товара"
            ),
        )
        query.SetParameter(
            "ТГ",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "ТГ"
            ),
        )
        query.SetParameter(
            "ТипТкани",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "статус по стратегии_OLAP"
            ),
        )
        query.SetParameter(
            "ДеливериСхем",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Схема доставки_OLAP"
            ),
        )
        query.SetParameter(
            "Объем",
            self.connection.ПланыВидовХарактеристик.СвойстваОбъектов.findbydescription(
                "Обьем, куб.м (для НОВОГО МЛ)*"
            ),
        )
        choose = query.execute().choose()
        liststock = []
        while choose.next():
            dict = {}
            dict["finished_product"] = choose.finished_product
            dict["actuality"] = choose.actuality
            dict["article"] = choose.article
            dict["nom_group"] = choose.nom_group
            dict["article_wb"] = choose.article_wb
            dict["name_marketplace"] = choose.name_marketplace
            dict["id_product_marketplace"] = choose.id_product_marketplace
            dict["Ad_barcode"] = choose.Ad_barcode
            dict["NewArticle"] = choose.NewArticle
            dict["Department"] = choose.Department
            dict["Counteragent"] = choose.Counteragent
            dict["Organisation"] = choose.Organisation
            dict["form"] = choose.form
            dict["textile"] = choose.textile
            dict["item"] = choose.item
            dict["fabric_type"] = choose.fabric_type
            dict["product_group"] = choose.product_group
            dict["product_status"] = choose.product_status
            dict["size"] = choose.size
            dict["print_color"] = choose.print_color
            dict["textile_n"] = choose.textile_n
            dict["textile_group"] = choose.textile_group
            dict["item_type"] = choose.item_type
            dict["strategy_status"] = choose.strategy_status
            dict["item_volume"] = choose.item_volume

            for_vpr = ""
            if dict["form"] is not None:
                for_vpr = dict["form"]
            if dict["fabric_type"] is not None:
                for_vpr = for_vpr + " " + dict["fabric_type"]

            dict["for_vpr"] = for_vpr
            dict["delivery_scheme"] = choose.delivery_scheme

            liststock.append(dict)
        return liststock

    def get_price_from_exchangeplan(self) -> list:
        if self.connection == None:
            raise "Нет подключения к базе 1С"
        NodeExchange = self.connection.ПланыОбмена.ОбменУправлениеПредприятиемРозничнаяТорговля.findBycode(
            "002"
        )
        self.connection.ПланыОбмена.SelectChanges(NodeExchange, 1)
        textquery = get_query_price_changes()
        query = self.connection.NewObject("Query", textquery)

        query.SetParameter(
            "Узел",
            NodeExchange,
        )
        query.SetParameter(
            "НомерСообщения",
            1,
        )

        choose = query.execute().choose()
        liststock = []
        while choose.next():
            dict = {}
            dict["finished_product"] = choose.finished_product
            dict["article"] = choose.article
            dict["price"] = choose.price
            dict["date_price"] = choose.date_price.date().isoformat()
            dict["price_name"] = choose.price_name
            dict["price_code"] = choose.price_code
            dict["dateExport"] = datetime.date.today().isoformat()
            dict["doc_guid"] = self.connection.xmlstring(choose.doc_ref)
            liststock.append(dict)
        return liststock

    def delete_changes_from_exchangeplan(self) -> None:
        if self.connection == None:
            raise "Нет подключения к базе 1С"
        NodeExchange = self.connection.ПланыОбмена.ОбменУправлениеПредприятиемРозничнаяТорговля.findBycode(
            "002"
        )
        self.connection.ПланыОбмена.DeleteChangeRecords(NodeExchange, 1)
        return None

    def get_stocks_for_marketplace(self, id_organisation, id_partner):
        if self.connection == None:
            raise "Нет подключения к базе 1С"
        textquery = get_query_stocks_for_marketplace()
        query = self.connection.NewObject("Query", textquery)

        query.SetParameter(
            "ОрганизацияКод",
            id_organisation,
        )
        query.SetParameter(
            "КонтрагентКод",
            id_partner,
        )

        choose = query.execute().choose()
        liststock = []
        while choose.next():
            dict = {}
            dict["id_sku"] = choose.id_sku
            dict["stock"] = choose.stock
            liststock.append(dict)
        return liststock

    def get_order_history_status(self) -> list:
        if self.connection == None:
            raise "Нет подключения к базе 1С"
        textquery = get_query_order_history_status()
        query = self.connection.NewObject("Query", textquery)

        choose = query.execute().choose()
        resultlist = []
        while choose.next():
            dict = {}
            dict["status_date"] = choose.status_date.isoformat()
            dict["status_name"] = choose.status_name
            dict["order_date"] = choose.order_date.date().isoformat()
            dict["order_number"] = choose.order_number
            dict["updated_by"] = choose.updated_by
            resultlist.append(dict)
        return resultlist

    def get_comission_report(self) -> list:
        if self.connection == None:
            raise "Нет подключения к базе 1С"
        textquery = get_query_documents_of_comission_report()
        query = self.connection.NewObject("Query", textquery)

        choose = query.execute().choose()
        resultlist = []
        while choose.next():
            dict = {}
            if choose.DateOrder != None:
                dict["DateOrder"] = choose.DateOrder.isoformat()
            if choose.DateReport != None:
                dict["DateReport"] = choose.DateReport.isoformat()
            if choose.DateSale != None:
                dict["DateSale"] = choose.DateSale.isoformat()

            dict["ArticulNom"] = choose.ArticulNom
            dict["CodeNom"] = choose.CodeNom
            dict["DescriptionNom"] = choose.DescriptionNom
            dict["IdNom"] = choose.IdNom

            dict["NumberOrder"] = choose.NumberOrder
            dict["NumberReport"] = choose.NumberReport
            dict["NumberSale"] = choose.NumberSale

            dict["Price"] = choose.Price
            dict["Qty"] = choose.Qty
            dict["Sum"] = choose.Sum
            dict["SumFee"] = choose.SumFee

            dict["Project"] = choose.Project
            dict["Department"] = choose.Department
            resultlist.append(dict)
        return resultlist

    def get_documents_of_service_receipt(self, dateStart, dateEnd) -> list:
        if self.connection == None:
            raise "Нет подключения к базе 1С"
        textquery = get_query_documents_of_service_receipt()
        query = self.connection.NewObject("Query", textquery)
        query.SetParameter(
            "НачалоПериода",
            self.connection.ValueFromStringInternal(
                f'{{"D",{dateStart.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y%m%d%H%M%S")}}}'
            ),
        )
        query.SetParameter(
            "КонецПериода",
            self.connection.ValueFromStringInternal(
                f'{{"D",{dateEnd.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y%m%d%H%M%S")}}}'
            ),
            self.connection.ВидГраницы.Включая,
        )

        choose = query.execute().choose()
        resultlist = []

        while choose.next():
            dict = {}
            dict["Amount"] = choose.Amount
            dict["Code_nom"] = choose.Code_nom
            dict["Customer"] = choose.Customer
            dict["Customer_cod"] = choose.Customer_cod
            dict["date_doc"] = choose.date_doc.isoformat()
            dict["date_incoming"] = choose.date_incoming.isoformat()
            dict["Department"] = choose.Department
            dict["Description_nom"] = choose.Description_nom
            dict["Doc_type"] = choose.Doc_type
            dict["number_doc"] = choose.number_doc
            dict["number_incoming"] = choose.number_incoming
            dict["Org"] = choose.Org
            dict["Quantity"] = choose.Quantity
            dict["Cost_item"] = choose.Сost_item
            dict["Cost_item_cod"] = choose.Сost_item_cod
            resultlist.append(dict)
        return resultlist

    def get_sale_by_period(self,dateStart,dateEnd) -> list:
        if self.connection == None:
            raise "Нет подключения к базе 1С"
        textquery = get_query_sale_by_period()
        query = self.connection.NewObject("Query", textquery)
        query.SetParameter(
            "BeginPeriod",
            self.connection.ValueFromStringInternal(
                f'{{"D",{dateStart.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y%m%d%H%M%S")}}}'
            ),
        )
        query.SetParameter(
            "EndPeriod",
            self.connection.ValueFromStringInternal(
                f'{{"D",{dateEnd.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y%m%d%H%M%S")}}}'
            ),
            self.connection.ВидГраницы.Включая,
        )
        choose = query.execute().choose()
        resultlist = []
        while choose.next():
            dict = {}
            if choose.DateOrder != None:
                dict["DateOrder"] = choose.DateOrder.isoformat()
            if choose.DateReport != None:
                dict["DateReport"] = choose.DateReport.isoformat()
            if choose.DateSale != None:
                dict["DateSale"] = choose.DateSale.isoformat()

            dict["ArticulNom"] = choose.ArticulNom
            dict["CodeNom"] = choose.CodeNom
            dict["DescriptionNom"] = choose.DescriptionNom
            dict["IdNom"] = choose.IdNom

            dict["NumberOrder"] = choose.NumberOrder
            dict["NumberReport"] = choose.NumberReport
            dict["NumberSale"] = choose.NumberSale

            dict["Price"] = choose.Price
            dict["Qty"] = choose.Qty
            dict["Sum"] = choose.Sum
            dict["SumFee"] = choose.SumFee

            dict["Project"] = choose.Project
            dict["Department"] = choose.Department
            resultlist.append(dict)
        return resultlist



def upload_from_1c(
    config, bqjsonservicefile, bqdataset, bqtable, datestock_start, datestock_end
):
    global query, choose
    struct_config = Struct(**config)
    CONSTR = f'Srvr="{struct_config.server}";Ref="{struct_config.infobase}";Usr="{struct_config.user}";Pwd="{struct_config.password}"'

    v83 = win32com.client.Dispatch("V83.COMConnector").Connect(CONSTR)
    q = get_query_fullstock()
    query = v83.NewObject("Query", q)

    filterList = []
    filterList.append(
        {
            "fieldname": "datestock",
            "operator": ">=",
            "value": datestock_start.strftime("%Y-%m-%d"),
        }
    )
    filterList.append(
        {
            "fieldname": "datestock",
            "operator": "<=",
            "value": datestock_end.strftime("%Y-%m-%d"),
        }
    )
    bq_method.DeleteRowFromTable(bqtable, bqdataset, bqjsonservicefile, filterList)

    for datestock in daterange(datestock_start, datestock_end):
        logger.info(f"Получение остатков из 1С {datestock.strftime('%d-%m-%Y')}.")
        query.SetParameter(
            "ДатаОстатковНачало",
            v83.newObject(
                "Граница",
                v83.ValueFromStringInternal(
                    f'{{"D",{datestock.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y%m%d%H%M%S")}}}'
                ),
                v83.ВидГраницы.Включая,
            ),
        )
        query.SetParameter(
            "ДатаОстатков",
            v83.newObject(
                "Граница",
                v83.ValueFromStringInternal(
                    f'{{"D",{datestock.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y%m%d%H%M%S")}}}'
                ),
                v83.ВидГраницы.Включая,
            ),
        )

        choose = query.execute().choose()

        liststock = []
        while choose.next():
            dict = {}
            dict["item"] = choose.item
            dict["articul"] = choose.articul
            dict["scl"] = choose.scl
            dict["item_group"] = choose.item_group
            dict["item_group_cost"] = choose.item_group_cost
            dict["item_type"] = choose.item_type
            dict["stock_start"] = choose.stock_start
            dict["stock_in"] = choose.stock_in
            dict["stock_out"] = choose.stock_out
            dict["stock_end"] = choose.stock_end
            dict["reserv"] = choose.reserv
            dict["delivering"] = choose.delivering
            dict["free_qty"] = choose.free_qty
            dict["ordered"] = choose.ordered
            dict["datestock"] = datestock.date().isoformat()
            dict["dateexport"] = datetime.date.today().isoformat()
            liststock.append(dict)
        csvfields = []
        csvfields.append({"item": "STRING"})
        csvfields.append({"articul": "STRING"})
        csvfields.append({"scl": "STRING"})
        csvfields.append({"item_group": "STRING"})
        csvfields.append({"item_group_cost": "STRING"})
        csvfields.append({"item_type": "STRING"})
        csvfields.append({"stock_start": "FLOAT"})
        csvfields.append({"stock_in": "FLOAT"})
        csvfields.append({"stock_out": "FLOAT"})
        csvfields.append({"stock_end": "FLOAT"})
        csvfields.append({"reserv": "FLOAT"})
        csvfields.append({"delivering": "FLOAT"})
        csvfields.append({"ordered": "FLOAT"})
        csvfields.append({"free_qty": "FLOAT"})
        csvfields.append({"datestock": "DATE"})
        csvfields.append({"dateexport": "DATE"})
        with open("personal.json", "w") as json_file:
            json.dump(liststock, json_file)

        bq_method.export_js_to_bq(
            liststock, bqtable, bqjsonservicefile, bqdataset, logger, csvfields
        )


def export_item_to_bq(fileconfi1c, bqjsonservicefile, bqdataset, bqtable):
    with open(fileconfi1c, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    cli = Client1c(config)
    cli.connect()
    liststock = cli.get_item_ref()
    if liststock == None:
        logger.critical("Нет подключения к базе 1С!")
        return
    if len(liststock) == 0:
        logger.critical("Нет данных в справочнике номенклатуры")
        return

    csvfields = []
    for key in liststock[0].keys():
        csvfields.append({key: "STRING"})
    #    with open("personal.json", "w") as json_file:
    #        json.dump(liststock, json_file)

    bq_method.TruncateTable(bqtable, bqdataset, bqjsonservicefile)

    bq_method.export_js_to_bq(
        liststock, bqtable, bqjsonservicefile, bqdataset, logger, csvfields
    )


def export_price_to_bq(fileconfi1c, bqjsonservicefile, bqdataset, bqtable):
    with open(fileconfi1c, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    cli = Client1c(config)
    cli.connect()
    liststock = cli.get_price_from_exchangeplan()
    if liststock == None:
        logger.critical("Нет подключения к базе 1С!")
        return
    if len(liststock) == 0:
        logger.info("Нет изменений цен")
        return

    csvfields = []
    for key in liststock[0].keys():
        if key == "price":
            type_field = "FLOAT"
        elif key in ("date_price", "dateExport"):
            type_field = "DATE"
        else:
            type_field = "STRING"
        csvfields.append({key: type_field})
    # with open("personal.json", "w") as json_file:
    #    json.dump(liststock, json_file)
    orderidlist = []
    orderidliststr = ""
    for elitems in liststock:
        if elitems["doc_guid"] not in orderidlist:
            orderidlist.append(elitems["doc_guid"])
            if orderidliststr != "":
                orderidliststr = orderidliststr + ","
            order_id = elitems["doc_guid"]
            orderidliststr = orderidliststr + f"'{order_id}'"

    filterList = []
    filterList.append(
        {
            "fieldname": "doc_guid",
            "operator": " IN ",
            "value": orderidliststr,
        }
    )
    bq_method.DeleteRowFromTable(bqtable, bqdataset, bqjsonservicefile, filterList)

    bq_method.export_js_to_bq(
        liststock, bqtable, bqjsonservicefile, bqdataset, logger, csvfields
    )
    cli.delete_changes_from_exchangeplan()


def export_order_status_history_from_1c2bq(
    config_1c, bqjsonservicefile, bqdataset, bqtable
):
    with open(config_1c, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    cli = Client1c(config)
    cli.connect()
    resultlist = cli.get_order_history_status()
    bq_method.export_js_to_bq(
        resultlist, bqtable, bqjsonservicefile, bqdataset, logger, []
    )


def export_documents_of_service_receipt_from_1c2bq(
    config_1c, bqjsonservicefile, bqdataset, bqtable, dateStart, dateEnd
):
    with open(config_1c, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    cli = Client1c(config)
    cli.connect()
    resultlist = cli.get_documents_of_service_receipt(dateStart, dateEnd)
    if len(resultlist) > 0:
        filterList = []
        filterList.append(
            {
                "fieldname": "date_doc",
                "operator": ">=",
                "value": dateStart.strftime("%Y-%m-%d"),
            }
        )
        filterList.append(
            {
                "fieldname": "date_doc",
                "operator": "<=",
                "value": dateEnd.strftime("%Y-%m-%d"),
            }
        )
        bq_method.DeleteRowFromTable(bqtable, bqdataset, bqjsonservicefile, filterList)

    bq_method.export_js_to_bq(
        resultlist, bqtable, bqjsonservicefile, bqdataset, logger, []
    )


def export_documents_commission_report_from_1c2bq(
    config_1c, bqjsonservicefile, bqdataset, bqtable
):
    with open(config_1c, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    cli = Client1c(config)
    cli.connect()
    resultlist = cli.get_comission_report()
    # if len(resultlist) > 0:
    # filterList = []
    # filterList.append(
    #     {
    #         "fieldname": "date_doc",
    #         "operator": ">=",
    #         "value": dateStart.strftime("%Y-%m-%d"),
    #     }
    # )
    # filterList.append(
    #     {
    #         "fieldname": "date_doc",
    #         "operator": "<=",
    #         "value": dateEnd.strftime("%Y-%m-%d"),
    #     }
    # )
    # bq_method.DeleteRowFromTable(bqtable, bqdataset, bqjsonservicefile, filterList)

    bq_method.export_js_to_bq(
        resultlist, bqtable, bqjsonservicefile, bqdataset, logger, []
    )
