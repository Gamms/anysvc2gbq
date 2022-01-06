import datetime
import tkinter as tk
from tkinter import *
from tkinter import ttk
from tkcalendar import DateEntry
import ozon_method
import bq_method
import yaml
from loguru import logger

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Google big query export")
        self.notebook = ttk.Notebook(self, width=500, height=100, padding=10)
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text='Ozon', underline=0,
                              sticky=tk.NE + tk.SW)
        frame_top=ttk.Frame(frame)
        frame_top.pack(side=TOP)
        frame_top1=ttk.Frame(frame)
        frame_top1.pack(side=TOP)

        b1 = ttk.Button(frame_top1,text="Ozon transaction v3")
        b1.bind("<Button-1>", self.ozon_update_transactionv3)
        b1.pack(side=TOP,padx=1,pady=1)

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
        bqdataset='OZON'
        bqjsonservicefile='polar.json'
        configyml='config_ozone.yml'
        with open(configyml) as f:
            config = yaml.safe_load(f)
        for lkConfig in config['lks']:
            ozonid=lkConfig['lk']['bq_id']
            apikey = lkConfig['lk']['apikey']
            clientid = lkConfig['lk']['clientid']

            logger.info(f'Начало импорта из OZON {ozonid}:')
            datefrom=self.date_from_element.get_date()
            dateto=self.date_to_element.get_date()

            try:
            #   js=ozon_method.ozon_import(apimethods.get(method),apikey,LOG_FILE,dateimport,maxdatechange)
                #clientid='44346'

                items=ozon_method.ozon_import(method,apimethods.get(method), apikey,clientid,ozonid,datefrom,dateto)
                if len(items)!=0:
                    logger.info(f'Чистим  данные в {bqtable} c {datefrom} по {dateto}')
                    fieldname = 'operation_date'
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
                    fields_list=ozon_method.fields_from_method(method)
                    bq_method.export_js_to_bq(items, bqtable, bqjsonservicefile, bqdataset,logger,fields_list)
                else:
                    logger.info(f'Данных нет {method} c {datefrom} по {dateto}')


            except Exception as e:
                logger.exception("Ошибка выполнения."+e.__str__())

        pass


if __name__ == "__main__":
    app = App()
    app.mainloop()
