from kivy.lang import Builder
from kivymd.app import MDApp
from datetime import date
from ozon_method import OzonDataFilterType
from transfer_method import transfer_orders_transaction_ozon2bq_in_the_period
from kivymd.uix.pickers import MDDatePicker

KV = '''
#:import MDTextField kivymd.uix.textfield.MDTextField
#:import MDRectangleFlatButton kivymd.uix.button

MDBoxLayout:
    orientation: 'vertical'
    MDTextField:
        id: start_date
        text: date.today().isoformat()
        mode: "rectangle"
        write_tab: False
        size_hint:.5,.1

    MDRaisedButton:
        text: "transaction_import"
        on_release: app.transaction_import()
        size_hint:.5,.1
'''

class MainApp(MDApp):
    def build(self):
        self.theme_cls.theme_style = "Dark"
        self.theme_cls.primary_palette = "Blue"

        return Builder.load_string(KV)

    def transaction_import(self):

        method = "transactionv3"
        bqtable = "tranv32022"
        fieldname = "operation_date"
        ozon_data_filter_type = OzonDataFilterType.date
        update_transaction_orders_by_period(
            bqtable, method, fieldname, ozon_data_filter_type,self.root.ids.start_date.text,date.today().isoformat()
        )

        pass

def update_transaction_orders_by_period(
    bqtable, method, fieldname, ozon_data_filter_type,datefrom,dateto
):
    bqdataset = "OZON"
    bqjsonservicefile = "polar.json"
    configyml = "config_ozon.yml"
    daterange = {"datefrom": date.fromisoformat(datefrom), "dateto": date.fromisoformat(dateto)}

    transfer_orders_transaction_ozon2bq_in_the_period(
        daterange,
        bqdataset,
        bqjsonservicefile,
        bqtable,
        configyml,
        fieldname,
        method,
        ozon_data_filter_type,
    )


MainApp().run()
