import datetime
import tkinter as tk
from tkinter import *
from tkinter import ttk

import bq_method
import ozon_method
import transfer_method
import wb_client
import yaml
from dateutil import parser
from loguru import logger
from tkcalendar import DateEntry


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Google big query export")
        self.notebook = ttk.Notebook(self, width=500, height=400, padding=10)
        self.add_frame_ozone()
        self.add_frame_wb()
        frame = ttk.Frame(self.notebook)

        self.notebook.add(frame, text="Verify", underline=0, sticky=tk.NE + tk.SW)
        self.label = ttk.Label(self)
        self.notebook.pack()
        self.label.pack(anchor=tk.W)
        self.notebook.enable_traversal()
        self.notebook.bind("<<NotebookTabChanged>>", self.select_tab)

    def add_frame_ozone(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Ozon", underline=0, sticky=tk.NE + tk.SW)
        frame_top = ttk.Frame(frame)
        frame_top.pack(side=TOP)
        frame_top1 = ttk.Frame(frame)
        frame_top1.pack(side=TOP)
        frame_top1left = ttk.Frame(frame_top1)
        frame_top1left.pack(side=LEFT)
        frame_top1right = ttk.Frame(frame_top1)
        frame_top1right.pack(side=RIGHT)
        b1 = ttk.Button(frame_top1left, text="Ozon transaction v3")
        b1.bind("<Button-1>", self.ozon_update_transactionv3)
        b1.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1right, text="Ozon orders v2 updated")
        b2.bind("<Button-1>", self.ozon_update_orders)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="Ozon fboorders_by_period")
        b2.bind("<Button-1>", self.ozon_update_fboorders_by_period)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="Ozon orders_by_period")
        b2.bind("<Button-1>", self.ozon_update_orders_by_period)
        b2.pack(side=TOP, padx=1, pady=1)
        ttk.Label(frame_top, text="Date from").pack(side=LEFT, padx=10, pady=10)
        self.date_from_element = DateEntry(
            frame_top,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        self.date_from_element.pack(side=LEFT, padx=10, pady=10)
        ttk.Label(frame_top, text="to").pack(side=LEFT, padx=10, pady=10)
        self.date_to_element = DateEntry(
            frame_top,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        self.date_to_element.pack(side=LEFT, padx=10, pady=10)

    def add_frame_wb(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="WB", underline=0, sticky=tk.NE + tk.SW)
        frame_top = ttk.Frame(frame)
        frame_top.pack(side=TOP)
        frame_top1 = ttk.Frame(frame)
        frame_top1.pack(side=TOP)
        frame_top1left = ttk.Frame(frame_top1)
        frame_top1left.pack(side=LEFT)
        frame_top1right = ttk.Frame(frame_top1)
        frame_top1right.pack(side=RIGHT)
        b1 = ttk.Button(frame_top1left, text="WB sale update")
        b1.bind("<Button-1>", self.wb_sale_update)
        b1.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1right, text="WB orders update")
        b2.bind("<Button-1>", self.wb_orders_update)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="WB sale period")
        b2.bind("<Button-1>", self.wb_sale_period)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="WB orders period")
        b2.bind("<Button-1>", self.wb_sale_update)
        b2.pack(side=TOP, padx=1, pady=1)
        ttk.Label(frame_top, text="Date from").pack(side=LEFT, padx=10, pady=10)
        self.date_from_element = DateEntry(
            frame_top,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        self.date_from_element.pack(side=LEFT, padx=10, pady=10)
        ttk.Label(frame_top, text="to").pack(side=LEFT, padx=10, pady=10)
        self.date_to_element = DateEntry(
            frame_top,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        self.date_to_element.pack(side=LEFT, padx=10, pady=10)

    def wb_orders_update(self, bt):
        method='orders'
        bqtable='orders'
        option='changes'
        wb_export(method,            bqtable,            option        )
        pass

    def wb_orders_period(self, bt):
        method='orders'
        bqtable='orders'
        option='byPeriod'
        datefrom = self.date_from_element.get_date()
        dateto = self.date_to_element.get_date()

        wb_export(method,            bqtable,            option,datefromstr=datefrom.isoformat(),datetostr=dateto.isoformat()       )
        pass

    def wb_sale_update(self, bt):
        method='sales'
        bqtable='sales'
        option='changes'
        wb_export(method,            bqtable,            option        )

        pass

    def wb_sale_period(self, bt):
        method='sales'
        bqtable='sales'
        option='byPeriod'
        datefrom = self.date_from_element.get_date()
        dateto = self.date_to_element.get_date()

        wb_export(method,            bqtable,            option ,datefromstr=datefrom.isoformat(),datetostr=dateto.isoformat()       )

        pass

    def select_tab(self, event):
        tab_id = self.notebook.select()
        tab_name = self.notebook.tab(tab_id, "text")
        text = f"Ваш текущий выбор: {tab_name}"
        self.label.config(text=text)

    def ozon_update_transactionv3(self, bt):
        apimethods = ozon_method.apimethods
        method = "transactionv3"
        bqtable = "tranv32022"
        fieldname = "operation_date"
        ozon_data_filter_type = ozon_method.OzonDataFilterType.date
        self.update_transaction_orders_by_period(
            bqtable, method, fieldname, ozon_data_filter_type
        )

        pass

    def ozon_update_fboorders_by_period(self, bt):
        apimethods = ozon_method.apimethods
        method = "fbo_orders"
        bqtable = "fbo_orders2021"
        fieldname = "created_at"
        ozon_data_filter_type = ozon_method.OzonDataFilterType.order_created_at
        self.update_transaction_orders_by_period(
            bqtable, method, fieldname, ozon_data_filter_type
        )

        pass

    def ozon_update_orders_by_period(self, bt):
        method = "orders"
        bqtable = "orders2021"
        fieldname = "created_at"
        ozon_data_filter_type = ozon_method.OzonDataFilterType.order_created_at
        self.update_transaction_orders_by_period(
            bqtable, method, fieldname, ozon_data_filter_type
        )

        pass

    def update_transaction_orders_by_period(
        self, bqtable, method, fieldname, ozon_data_filter_type
    ):
        bqdataset = "OZON"
        bqjsonservicefile = "polar.json"
        configyml = "config_ozone.yml"
        datefrom = self.date_from_element.get_date()
        dateto = self.date_to_element.get_date()
        daterange = {"datefrom": datefrom, "dateto": dateto}

        transfer_method.transfer_orders_transaction_ozon2bq_in_the_period(
            daterange,
            bqdataset,
            bqjsonservicefile,
            bqtable,
            configyml,
            fieldname,
            method,
            ozon_data_filter_type,
        )

    def ozon_update_orders(self, bt):
        method = "orders"
        bqtable = "orders2021"
        bqdataset = "OZON"
        bqjsonservicefile = "polar.json"
        configyml = "config_ozone.yml"
        datefrom = self.date_from_element.get_date()
        dateto = self.date_to_element.get_date()
        daterange = {"datefrom": datefrom, "dateto": dateto}
        textresult = transfer_method.export_orders_from_ozon2bq_updated_in_the_period(
            daterange, bqdataset, bqjsonservicefile, bqtable, configyml, method
        )


def wb_export(
    method,
    bqtable,
    option,
    jsonkey="polar.json",
    datasetid="WB",
    configyml="config_wb.yml",
    datefromstr="",
    datetostr="",
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
                datefrom = datetime.date.today()
                dateto = datefrom
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
                )
                logger.info(f"Загружаем записи {method} из WB с {datefrom}:")
                bq_method.export_js_to_bq(
                    orders, bqtable, jsonkey, datasetid, logger, []
                )
            else:
                logger.info("Нет данных")
            logger.info(f"end")


def clean_table_if_necessary(
    datasetid,
    datefrom,
    dateto,
    field_date,
    jsonkey,
    loger,
    method,
    tablebq,
    wb_id,
    option,
):
    filterList = []
    if option == "byPeriod":
        filterList.append(
            {
                "fieldname": field_date,
                "operator": ">=",
                "value": datefrom.strftime("%Y-%m-%d"),
            }
        )
        filterList.append(
            {
                "fieldname": field_date,
                "operator": "<=",
                "value": dateto.strftime("%Y-%m-%d"),
            }
        )
        filterList.append({"fieldname": "wb_id", "operator": "=", "value": wb_id})
        loger.info(f"чистим записи {method} в bq с {datefrom}:")
        bq_method.DeleteRowFromTable(tablebq, datasetid, jsonkey, filterList)


def fill_date(
    option,
    tablebq,
    datasetid,
    jsonkey,
    wb_id,
    field_date,
    method,
    datefromstr="",
    datetostr="",
):
    if datetostr == "":
        dateto = datetime.datetime.today()
    else:
        dateto = parser.parse(datetostr)

    if datefromstr == "":
        datefrom = datetime.datetime(dateto.year, dateto.month - 1, 1, 0, 0, 0)
    else:
        datefrom = parser.parse(datefromstr)
    if option == "changes":
        maxdatechange = bq_method.GetMaxRecord(
            tablebq, datasetid, jsonkey, wb_id, field_date
        )
        if maxdatechange.replace(tzinfo=None) > datefrom:
            datefrom = maxdatechange.replace(tzinfo=None)

    return datefrom, dateto


if __name__ == "__main__":
    app = App()
    app.mainloop()
