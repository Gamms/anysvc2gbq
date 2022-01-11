import datetime
import tkinter as tk
from tkinter import *
from tkinter import ttk
from tkcalendar import DateEntry
import ozon_method
import bq_method
import yaml
from loguru import logger
import transfer_method
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Google big query export")
        self.notebook = ttk.Notebook(self, width=500, height=400, padding=10)
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text='Ozon', underline=0,
                              sticky=tk.NE + tk.SW)
        frame_top=ttk.Frame(frame)
        frame_top.pack(side=TOP)
        frame_top1=ttk.Frame(frame)
        frame_top1.pack(side=TOP)
        frame_top1left=ttk.Frame(frame_top1)
        frame_top1left.pack(side=LEFT)
        frame_top1right=ttk.Frame(frame_top1)
        frame_top1right.pack(side=RIGHT)

        b1 = ttk.Button(frame_top1left,text="Ozon transaction v3")
        b1.bind("<Button-1>", self.ozon_update_transactionv3)
        b1.pack(side=TOP,padx=1,pady=1)

        b2 = ttk.Button(frame_top1right,text="Ozon orders v2 updated")
        b2.bind("<Button-1>", self.ozon_update_orders)
        b2.pack(side=TOP,padx=1,pady=1)
        b2 = ttk.Button(frame_top1left,text="Ozon fboorders_by_period")
        b2.bind("<Button-1>", self.ozon_update_fboorders_by_period)
        b2.pack(side=TOP,padx=1,pady=1)
        b2 = ttk.Button(frame_top1left,text="Ozon orders_by_period")
        b2.bind("<Button-1>", self.ozon_update_orders_by_period)
        b2.pack(side=TOP,padx=1,pady=1)

        ttk.Label(frame_top, text='Date from').pack(side=LEFT, padx=10, pady=10)
        self.date_from_element = DateEntry(frame_top,locale='ru_RU', date_pattern='dd-mm-y', width=12, background='darkblue',
                        foreground='white', borderwidth=2)
        self.date_from_element.pack(side=LEFT,padx=10, pady=10)
        ttk.Label(frame_top, text='to').pack(side=LEFT, padx=10, pady=10)
        self.date_to_element = DateEntry(frame_top,locale='ru_RU', date_pattern='dd-mm-y', width=12, background='darkblue',
                        foreground='white', borderwidth=2)
        self.date_to_element.pack(side=LEFT,padx=10, pady=10)

        frame = ttk.Frame(self.notebook)

        self.notebook.add(frame, text='Verify', underline=0,
                              sticky=tk.NE + tk.SW)
        self.label = ttk.Label(self)
        self.notebook.pack()
        self.label.pack(anchor=tk.W)
        self.notebook.enable_traversal()
        self.notebook.bind("<<NotebookTabChanged>>", self.select_tab)

    def select_tab(self, event):
        tab_id = self.notebook.select()
        tab_name = self.notebook.tab(tab_id, "text")
        text = "Ваш текущий выбор: {}".format(tab_name)
        self.label.config(text=text)

    def ozon_update_transactionv3(self,bt):
        apimethods = ozon_method.apimethods
        method='transactionv3'
        bqtable='tranv32022'
        fieldname = 'operation_date'
        ozon_data_filter_type = ozon_method.OzonDataFilterType.date
        self.update_transaction_orders_by_period(bqtable, method,fieldname,ozon_data_filter_type)

        pass
    def ozon_update_fboorders_by_period(self,bt):
        apimethods = ozon_method.apimethods
        method='fbo_orders'
        bqtable='fbo_orders2021'
        fieldname = 'created_at'
        ozon_data_filter_type = ozon_method.OzonDataFilterType.order_created_at
        self.update_transaction_orders_by_period(bqtable, method,fieldname,ozon_data_filter_type)

        pass
    def ozon_update_orders_by_period(self,bt):
        method='orders'
        bqtable='orders2021'
        fieldname = 'created_at'
        ozon_data_filter_type = ozon_method.OzonDataFilterType.order_created_at
        self.update_transaction_orders_by_period(bqtable, method,fieldname,ozon_data_filter_type)

        pass

    def update_transaction_orders_by_period(self,  bqtable, method,                    fieldname,ozon_data_filter_type):
        bqdataset = 'OZON'
        bqjsonservicefile = 'polar.json'
        configyml = 'config_ozone.yml'
        datefrom = self.date_from_element.get_date()
        dateto = self.date_to_element.get_date()
        daterange={'datefrom':datefrom,'dateto':dateto}

        transfer_method.transfer_orders_transaction_ozon2bq_in_the_period(daterange,bqdataset, bqjsonservicefile, bqtable, configyml,
                                                                fieldname, method, ozon_data_filter_type)


    def ozon_update_orders(self,bt):
        method='orders'
        bqtable='orders2021'
        bqdataset='OZON'
        bqjsonservicefile='polar.json'
        configyml='config_ozone.yml'
        datefrom = self.date_from_element.get_date()
        dateto = self.date_to_element.get_date()
        daterange={'datefrom':datefrom,'dateto':dateto}
        textresult=transfer_method.export_orders_from_ozon2bq_updated_in_the_period(daterange, bqdataset, bqjsonservicefile, bqtable,
                                                              configyml, method)





if __name__ == "__main__":
    app = App()
    app.mainloop()
