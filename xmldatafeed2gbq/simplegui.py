import logging
import queue
import tkinter as tk
from tkinter import *
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

import bq_method
import method_telegram
import ozon_method
import transfer_method
import verifydata
from client1c import export_documents_commission_report_from_1c2bq
from loguru import logger
from tkcalendar import DateEntry
from tkinter import filedialog as fd
import pandas as pd
import openpyxl
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
        self.order_id = tk.IntVar()
        self.order_id.set(546463554)
        self.ozon_id = tk.StringVar()
        self.ozon_id.set("ip_bog")

        self.title("Google big query export")
        self.notebook = ttk.Notebook(self, width=500, height=400, padding=10)
        self.entryList = []
        self.entry_dict = {}

        self.add_frame_ozone()
        self.add_frame_wb()
        self.add_frame_ym()
        self.add_frame_verify()
        self.add_frame_1C()
        self.add_frame_DetMir()
        frame = ttk.Frame(self.notebook)

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
        self.filename=''
    def add_frame_verify(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Verify data", underline=0, sticky=tk.NE + tk.SW)
        frame_top = ttk.Frame(frame)
        frame_top.pack(side=TOP)
        frame_top1 = ttk.Frame(frame)
        frame_top1.pack(side=TOP)
        frame_top1left = ttk.Frame(frame_top1)
        frame_top1left.pack(side=LEFT)
        frame_top1right = ttk.Frame(frame_top1)
        frame_top1right.pack(side=RIGHT)
        b1 = ttk.Button(frame_top1left, text="Verify ")
        b1.bind("<Button-1>", self.verify)
        b1.pack(side=TOP, padx=1, pady=1)

    def verify(self, element):
        verifydata.verify()
        pass

    def add_buton_on_frame(self, frame, text, func, side, padx, pady):
        b1 = ttk.Button(frame, text=text)
        b1.bind("<Button-1>", func)
        b1.pack(side=side, padx=padx, pady=pady)

    def add_frame_ym(self):
        frame = ttk.Frame(self.notebook)
        entrydict = {}
        self.notebook.add(frame, text="YANDEX", underline=0, sticky=tk.NE + tk.SW)
        frame_top = ttk.Frame(frame)
        frame_top.pack(side=TOP)
        frame_top1 = ttk.Frame(frame)
        frame_top1.pack(side=TOP)
        frame_top1left = ttk.Frame(frame_top1)
        frame_top1left.pack(side=LEFT)
        frame_top1right = ttk.Frame(frame_top1)
        frame_top1right.pack(side=RIGHT)
        b1 = ttk.Button(frame_top1left, text="Yandex orders update orders changes")
        b1.bind("<Button-1>", self.ym_update_orders)
        b1.pack(side=TOP, padx=1, pady=1)

        self.add_buton_on_frame(
            frame_top1left,
            "Yandex orders update on period",
            self.ym_period_orders,
            TOP,
            1,
            1,
        )

        b1 = ttk.Button(frame_top1left, text="Stocks 1c->YM")
        b1.bind("<Button-1>", self.ym_export_stocks)
        b1.pack(side=TOP, padx=1, pady=1)

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
        self.entry_dict["frame_ym"] = entrydict

    def ym_update_orders(self, el):
        datefrom, dateto = self.get_date_frame("frame_ym")
        transfer_method.export_orders_from_ym2bq()
        pass

    def ym_period_orders(self, el):
        datefrom, dateto = self.get_date_frame("frame_ym")
        transfer_method.export_orders_from_ym2bq(dateFrom=datefrom, dateTo=dateto)
        pass

    def ym_export_stocks(self):
        fileconfig1c = "client1C_config.yml"
        fileconfigyandex = "config_yandex.yml"
        transfer_method.export_stocks_from_1c2ym(fileconfig1c, fileconfigyandex)

    def comission_report_1c(self, arg):
        bqjsonservicefile = "polar.json"
        bqdataset = "DB1C"
        bqtable = "CommissionReport"
        fileconfig1c = "client1C_config.yml"
        export_documents_commission_report_from_1c2bq(
            fileconfig1c,
            bqjsonservicefile,
            bqdataset,
            bqtable,
        )

    def sale_1c(self, bt):
        bqjsonservicefile = "polar.json"
        bqdataset = "DB1C"
        bqtable = "Sale"
        fileconfig1c = "client1C_config.yml"
        datefrom, dateto = self.get_date_frame("frame_1C")
        transfer_method.export_sale_from_1c2bq(
            config_1c=fileconfig1c,
            bqjsonservicefile=bqjsonservicefile,
            bqdataset=bqdataset,
            bqtable=bqtable,
            dateStart=datefrom,
            dateEnd=dateto
        )

        pass

    def chooseFileXls(self,bt):
        self.filename = fd.askopenfilename(defaultextension=".xls",filetypes=[("All types",".*"),("XLS",".xls"),("XLSX",".xlsx")])
        print(self.filename)
        self.labelFile.config(text=self.filename)


    def sale_info_detmir(self,bt):
        dataExcel=pd.read_excel(self.filename,usecols='A:T')
        dic=dataExcel.to_dict('records')
        a = str(dic[0])
        endstr = a.find(' с ')
        Org=a[29:57]
        frame_entry_dict = self.entry_dict['frame_DetMir']
        date_element = frame_entry_dict["date_element"]
        date = date_element.get_date()

        listColumn=['name','brand','code','subseason','season','articul','shk','size','numberContract',\
                    'inQty','inValue','orderQty','orderValue','outstandingQty','outstandingValue','saleQty','saleValue','outQty','outValue','stock']
        newlist=[]
        for i in range(2,len(dic)-1):
            newdic = {}
            for value, key in zip(dic[i].values(), listColumn):
                if str(value)=='nan':
                    value=''
                if key.find('Value')>0:
                    value=float(value)
                newdic[key]=value
            newdic['date']=date.isoformat()
            newdic['org'] =Org

            newlist.append(newdic)
        transfer_method.export_from_dict_to_gbq(newlist,"polar.json","DetMir", "Sale")

        pass
    def add_frame_DetMir(self):
        frame = ttk.Frame(self.notebook)
        entrydict = {}
        self.notebook.add(frame, text="Детский мир", underline=0, sticky=tk.NE + tk.SW)
        frame_top = ttk.Frame(frame)
        frame_top.pack(side=TOP)
        frame_top1 = ttk.Frame(frame)
        frame_top1.pack(side=TOP)
        frame_top1left = ttk.Frame(frame_top1)
        frame_top1left.pack(side=LEFT)
        frame_top1right = ttk.Frame(frame_top1)
        frame_top1right.pack(side=RIGHT)
        frame_top2 = ttk.Frame(frame_top1left)
        frame_top2.pack(side=TOP)

        b1 = ttk.Button(frame_top2, text="Выбрать файл Excel с данными о продаже")
        b1.bind("<Button-1>", self.chooseFileXls)
        b1.pack(side=LEFT, padx=1, pady=1)
        self.labelFile=ttk.Label(frame_top, text="Date from")
        self.labelFile.pack(side=LEFT, padx=10, pady=10)
        self.labelFile.config(text='Выберите файл')
        b1 = ttk.Button(frame_top2, text="Загрузка из Excel данных о продажах")
        b1.bind("<Button-1>", self.sale_info_detmir)
        b1.pack(side=LEFT, padx=1, pady=1)

        ttk.Label(frame_top1left, text="Дата операций").pack(side=LEFT, padx=10, pady=10)
        date_from_element = DateEntry(
            frame_top1left,
            locale="ru_RU",
            date_pattern="dd-mm-y",
            width=12,
            background="darkblue",
            foreground="white",
            borderwidth=2,
        )
        date_from_element.pack(side=LEFT, padx=10, pady=10)
        entrydict = {}
        entrydict["date_element"] = date_from_element
        self.entry_dict["frame_DetMir"] = entrydict

    def add_frame_1C(self):
        frame = ttk.Frame(self.notebook)
        entrydict = {}
        self.notebook.add(frame, text="1C", underline=0, sticky=tk.NE + tk.SW)
        frame_top = ttk.Frame(frame)
        frame_top.pack(side=TOP)
        frame_top1 = ttk.Frame(frame)
        frame_top1.pack(side=TOP)
        frame_top1left = ttk.Frame(frame_top1)
        frame_top1left.pack(side=LEFT)
        frame_top1right = ttk.Frame(frame_top1)
        frame_top1right.pack(side=RIGHT)
        frame_top2 = ttk.Frame(frame_top1left)
        frame_top2.pack(side=TOP)
        b1 = ttk.Button(frame_top2, text="1C Отчеты комиссионеров (DB1C.ComissionReport)")
        b1.bind("<Button-1>", self.comission_report_1c)
        b1.pack(side=LEFT, padx=1, pady=1)
        frame_top3 = ttk.Frame(frame_top1left)
        frame_top3.pack(side=TOP)
        b1 = ttk.Button(frame_top3, text="1C Продажи за период (DB1C.Sale)")
        b1.bind("<Button-1>", self.sale_1c)
        b1.pack(side=RIGHT, padx=1, pady=1)

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
        self.entry_dict["frame_1C"] = entrydict

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
        self.add_buton_on_frame(
            frame_top1left,
            "Ozon transaction v3",
            self.ozon_update_transactionv3,
            TOP,
            1,
            1,
        )
        self.add_buton_on_frame(
            frame_top1right,
            "Ozon orders v2 updated",
            self.ozon_update_transactionv3,
            TOP,
            1,
            1,
        )
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
        b2 = ttk.Button(frame_top1left, text="Update stocks v3")
        b2.bind("<Button-1>", self.ozon_update_stocks_v3)
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

    def get_max_wb_reportsale(self, bt):
        field = "rr_dt"
        maxdatechange = bq_method.GetMaxRecord(
            "reportsale", "wb", "polar.json", "", field
        )
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
        b2 = ttk.Button(frame_top1left, text="WB orders V3 period")
        b2.bind("<Button-1>", self.wb_ordersv2_period)
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

        b2 = ttk.Button(frame_top1left, text="Get max from wb reportsale")
        b2.bind("<Button-1>", self.get_max_wb_reportsale)
        b2.pack(side=TOP, padx=1, pady=1)

        self.add_buton_on_frame(
            frame_top1right, "WB INVOICE updated", self.wb_invoice_update, TOP, 1, 1
        )

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

    def wb_invoice_update(self, bt):
        method = "invoice_v1"
        bqtable = "invoice"
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

    def wb_ordersv2_period(self, bt):
        method = "ordersv3"
        bqtable = "ordersv3"
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
        order_id = self.order_id.get()
        ozon_id = self.ozon_id.get()
        ozon_data_filter_type = ozon_method.OzonDataFilterType.order_id
        transfer_method.export_orders_from_ozon2bq_by_id(
            "OZON", "polar.json", "orders2021", "config_ozon.yml", order_id, ozon_id
        )

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

    def ozon_update_stocks_v3(self, bt):
        bqtable = "stocks_v3"
        bqdataset = "OZON"
        bqjsonservicefile = "polar.json"
        configyml = "config_ozon.yml"
        textresult = transfer_method.export_stocks_from_ozon2bq(
            bqdataset, bqjsonservicefile, bqtable, configyml
        )


if __name__ == "__main__":
    tg_handler = method_telegram.get_loguru_telegramm_notification_handler(
        logger, "-1001572341087", "2028570019:AAEhd5gfY6qxZRmJZfymO82xSO4E-VuMXjU"
    )
    if tg_handler != None:
        logger.add(tg_handler, level="ERROR")
    app = App()
    app.mainloop()
