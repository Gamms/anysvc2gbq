import datetime
import logging
import queue
import tkinter as tk
from tkinter import *
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

import bq_method
import ozon_method
import transfer_method
import wb_client
import yaml
from client1c import daterange
from dateutil import parser
from dateutil.relativedelta import relativedelta
from loguru import logger
from tkcalendar import DateEntry




class QueueHandler(logging.Handler):
    """Class to send logging records to a queue
    It can be used from different threads
    The ConsoleUi class polls this queue to display records in a ScrolledText widget
    """

    # Example from Moshe Kaplan: https://gist.github.com/moshekaplan/c425f861de7bbf28ef06
    # (https://stackoverflow.com/questions/13318742/python-logging-to-tkinter-text-widget) is not thread safe!
    # See https://stackoverflow.com/questions/43909849/tkinter-python-crashes-on-new-thread-trying-to-log-on-main-thread

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(record)


class ConsoleUi:
    """Poll messages from a logging queue and display them in a scrolled text widget"""

    def __init__(self, frame):
        self.frame = frame
        # Create a ScrolledText wdiget
        self.scrolled_text = ScrolledText(frame, state="disabled", height=12)
        self.scrolled_text.grid(row=0, column=0, sticky=(N, S, W, E))
        self.scrolled_text.configure(font="TkFixedFont")
        self.scrolled_text.tag_config("INFO", foreground="black")
        self.scrolled_text.tag_config("DEBUG", foreground="gray")
        self.scrolled_text.tag_config("WARNING", foreground="orange")
        self.scrolled_text.tag_config("ERROR", foreground="red")
        self.scrolled_text.tag_config("CRITICAL", foreground="red", underline=1)
        # Create a logging handler using a queue
        self.log_queue = queue.Queue()
        self.queue_handler = QueueHandler(self.log_queue)

        logger.add(self.queue_handler)

        self.frame.after(100, self.poll_log_queue)

    def display(self, record):
        msg = self.queue_handler.format(record)
        self.scrolled_text.configure(state="normal")
        self.scrolled_text.insert(tk.END, msg + "\n", record.levelname)
        self.scrolled_text.configure(state="disabled")
        # Autoscroll to the bottom
        self.scrolled_text.yview(tk.END)

    def poll_log_queue(self):
        # Check every 100ms if there is a new message in the queue to display
        while True:
            try:
                record = self.log_queue.get(block=False)
            except queue.Empty:
                break
            else:
                self.display(record)
        self.frame.after(100, self.poll_log_queue)


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.order_id=tk.IntVar()
        self.order_id.set(546463554)
        self.ozon_id=tk.StringVar()
        self.ozon_id.set('ip_bog')

        self.title("Google big query export")
        self.notebook = ttk.Notebook(self, width=500, height=400, padding=10)
        self.entryList = []
        self.entry_dict = {}

        self.add_frame_ozone()
        self.add_frame_wb()
        frame = ttk.Frame(self.notebook)

        self.notebook.add(frame, text="Verify", underline=0, sticky=tk.NE + tk.SW)
        self.label = ttk.Label(self)
        self.notebook.pack()
        self.label.pack(anchor=tk.W)
        self.notebook.enable_traversal()
        self.notebook.bind("<<NotebookTabChanged>>", self.select_tab)

        console_frame = ttk.Labelframe(self, text="Console")
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)
        self.console = ConsoleUi(console_frame)
        self.console.frame.pack()

    def add_frame_ozone(self):
        frame = ttk.Frame(self.notebook)
        entrydict = {}
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
        b2 = ttk.Button(frame_top1left, text="Get max from ozone orders")
        b2.bind("<Button-1>", self.get_max_ozon_orders)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="Get max from ozone transaction")
        b2.bind("<Button-1>", self.get_max_ozon_trn)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="Update orders by id")
        b2.bind("<Button-1>", self.ozon_update_orders_by_id)
        b2.pack(side=TOP, padx=1, pady=1)
        e = Entry(frame_top1left, textvariable=self.order_id)
        e.pack()
        e = Entry(frame_top1left, textvariable=self.ozon_id)
        e.pack()




        ttk.Label(frame_top, text="Date from").pack(side=LEFT, padx=10, pady=10)
        date_from_element = DateEntry(
            frame_top,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        date_from_element.pack(side=LEFT, padx=10, pady=10)
        ttk.Label(frame_top, text="to").pack(side=LEFT, padx=10, pady=10)
        date_to_element = DateEntry(
            frame_top,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        date_to_element.pack(side=LEFT, padx=10, pady=10)
        entrydict["date_from_element"] = date_from_element
        entrydict["date_to_element"] = date_to_element
        self.entry_dict["frame_ozone"] = entrydict

    def get_max_ozon_orders(self, bt):
        maxdatechange = bq_method.GetMaxRecord(
            "orders2021", "OZON", "polar.json", "", "created_at"
        )
        logger.debug(maxdatechange)

    def get_max_ozon_trn(self, bt):
        maxdatechange = bq_method.GetMaxRecord(
            "tranv32022", "OZON", "polar.json", "", "operation_date"
        )
        logger.debug(maxdatechange)

    def get_max_wb_sale(self, bt):
        field = "lastChangeDate"
        maxdatechange = bq_method.GetMaxRecord("sales", "wb", "polar.json", "", field)
        logger.debug(f"{field} :{maxdatechange}")

    def get_max_wb_orders(self, bt):
        field = "lastChangeDate"
        maxdatechange = bq_method.GetMaxRecord("orders", "wb", "polar.json", "", field)
        logger.debug(f"{field} :{maxdatechange}")

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
        b2.bind("<Button-1>", self.wb_orders_period)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="WB reportsale period")
        b2.bind("<Button-1>", self.wb_reportsale_period)
        b2.pack(side=TOP, padx=1, pady=1)

        b2 = ttk.Button(frame_top1left, text="WB stock")
        b2.bind("<Button-1>", self.wb_stock)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="WB stock v1 period")
        b2.bind("<Button-1>", self.wb_stock_period)
        b2.pack(side=TOP, padx=1, pady=1)

        b2 = ttk.Button(frame_top1left, text="Get max from wb orders")
        b2.bind("<Button-1>", self.get_max_wb_orders)
        b2.pack(side=TOP, padx=1, pady=1)
        b2 = ttk.Button(frame_top1left, text="Get max from wb sale")
        b2.bind("<Button-1>", self.get_max_wb_sale)
        b2.pack(side=TOP, padx=1, pady=1)

        ttk.Label(frame_top, text="Date from").pack(side=LEFT, padx=10, pady=10)
        date_from_element = DateEntry(
            frame_top,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        date_from_element.pack(side=LEFT, padx=10, pady=10)
        ttk.Label(frame_top, text="to").pack(side=LEFT, padx=10, pady=10)
        date_to_element = DateEntry(
            frame_top,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        date_to_element.pack(side=LEFT, padx=10, pady=10)
        entrydict = {}
        entrydict["date_from_element"] = date_from_element
        entrydict["date_to_element"] = date_to_element
        self.entry_dict["frame_wb"] = entrydict

    def get_date_frame(self, nameframe):
        frame_entry_dict = self.entry_dict[nameframe]
        date_from_element = frame_entry_dict["date_from_element"]
        date_to_element = frame_entry_dict["date_to_element"]
        datefrom = date_from_element.get_date()
        dateto = date_to_element.get_date()
        return datefrom, dateto

    def wb_orders_update(self, bt):
        method = "orders"
        bqtable = "orders"
        option = "changes"
        transfer_method.wb_export(method, bqtable, option)
        pass

    def wb_orders_period(self, bt):
        method = "orders"
        bqtable = "orders"
        option = "byPeriod"
        datefrom, dateto = self.get_date_frame("frame_wb")
        transfer_method.wb_export(
            method,
            bqtable,
            option,
            datefromstr=datefrom.isoformat(),
            datetostr=dateto.isoformat(),
        )
        pass

    def wb_reportsale_period(self, bt):
        method = "reportsale"
        bqtable = "reportsale"
        option = "byPeriod"
        datefrom, dateto = self.get_date_frame("frame_wb")
        transfer_method.wb_export(
            method,
            bqtable,
            option,
            datefromstr=datefrom.isoformat(),
            datetostr=dateto.isoformat(),
        )
        pass

    def wb_sale_update(self, bt):
        method = "sales"
        bqtable = "sales"
        option = "changes"
        transfer_method.wb_export(method, bqtable, option)

        pass

    def wb_sale_period(self, bt):
        method = "sales"
        bqtable = "sales"
        option = "byPeriod"
        datefrom, dateto = self.get_date_frame("frame_wb")
        transfer_method.wb_export(
            method,
            bqtable,
            option,
            datefromstr=datefrom.isoformat(),
            datetostr=dateto.isoformat(),
        )

        pass

    def wb_stock_period(self, bt):
        method = "stocks_v1"
        bqtable = "stocks_v1"
        datefrom, dateto = self.get_date_frame("frame_wb")
        transfer_method.wb_export(method, bqtable, datefrom=datefrom, dateto=dateto)

    def wb_stock(self, bt):
        method = "stocks_v1"
        bqtable = "stocks_v1"
        transfer_method.wb_export(method, bqtable, datefrom=datestock, dateto=datestock)

        method = "stocks_v2"
        bqtable = "stocks_v2"
        transfer_method.wb_export(method, bqtable)

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
        ozon_data_filter_type = ozon_method.OzonDataFilterType.since
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

    def ozon_update_orders_by_id(self, bt):
        method = "orders"
        bqtable = "orders2021"
        fieldname = "created_at"
        order_id=self.order_id.get()
        ozon_id=self.ozon_id.get()
        ozon_data_filter_type = ozon_method.OzonDataFilterType.order_id
        transfer_method.export_orders_from_ozon2bq_by_id("OZON", "polar.json", "orders2021","config_ozon.yml",order_id,ozon_id)


        pass


    def update_transaction_orders_by_period(
        self, bqtable, method, fieldname, ozon_data_filter_type
    ):
        bqdataset = "OZON"
        bqjsonservicefile = "polar.json"
        configyml = "config_ozon.yml"
        datefrom, dateto = self.get_date_frame("frame_ozone")
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
        configyml = "config_ozon.yml"
        datefrom, dateto = self.get_date_frame("frame_ozone")
        daterange = {"datefrom": datefrom, "dateto": dateto}
        textresult = transfer_method.export_orders_from_ozon2bq_updated_in_the_period(
            daterange, bqdataset, bqjsonservicefile, bqtable, configyml, method
        )


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
    items,
):
    filterList = []
    if method in ("stocks_v1", "stocks_v2"):
        filterList.append({"fieldname": "wb_id", "operator": "=", "value": wb_id})
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
        loger.info(f"чистим записи {method} в bq с {datefrom}:")
        bq_method.DeleteRowFromTable(tablebq, datasetid, jsonkey, filterList)

    elif option == "byPeriod":
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
    elif option == "changes":
        logger.info(f"Чистим  данные в {tablebq} по {len(items)} заказам")
        fieldname = "operation_date"
        filterList = []
        filterList.append(
            {
                "fieldname": "wb_id",
                "operator": "=",
                "value": wb_id,
            }
        )
        orderidlist = ""
        for elitems in items:
            if orderidlist != "":
                orderidlist = orderidlist + ","
            odid = elitems["odid"]
            orderidlist = orderidlist + f"'{odid}'"

        filterList.append(
            {
                "fieldname": "odid",
                "operator": " IN ",
                "value": orderidlist,
            }
        )
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
        datefrom = dateto - relativedelta(months=1)
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
