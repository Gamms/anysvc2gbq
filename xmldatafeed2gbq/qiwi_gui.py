import os
from datetime import date

from kivy.lang import Builder
from kivymd.app import MDApp
from kivymd.uix.pickers import MDDatePicker
from ozon_method import OzonDataFilterType
from transfer_method import transfer_orders_transaction_ozon2bq_in_the_period

os.environ["KIVY_GL_BACKEND"] = "angle_sdl2"

KV = """
#:import MDTextField kivymd.uix.textfield.MDTextField
#:import MDRectangleFlatButton kivymd.uix.button
#:import TabbedPanel kivy.uix.tabbedpanel
#:import hex kivy.utils.get_color_from_hex
<TabbedPanelStrip>
    canvas:
        Color:
            rgba: hex('#D3FF5E')
        Rectangle:
            size: self.size
            pos: self.pos
MDBoxLayout:
    orientation: "vertical"
    MDTopAppBar:
        md_bg_color:hex('#151D91')
        specific_text_color: hex('#D3FF5E')
        right_action_items: [["dots-vertical", lambda x: app.callback()]]
        title: "Any service Export"
    MDBoxLayout:
        spacing: "5dp"
        TabbedPanel:
            do_default_tab: False

            TabbedPanelItem:
                text: "OZON"
                background_color:hex('#151D91')
                border:(15,15,15,15)
                MDBoxLayout:
                    MDBoxLayout:
                        orientation: 'vertical'
                        spacing: "5dp"
                        size: self.minimum_size
                        pos_hint: {"top": 1}
                        MDTextField:
                            id: start_date
                            text: date.today().isoformat()
                            mode: "rectangle"
                            write_tab: False
                            pos_hint: {"center_y": .9}
                        MDTextField:
                            id: end_date
                            text: date.today().isoformat()
                            mode: "rectangle"
                            write_tab: False
                            pos_hint: {"center_y": .9}

                        MDRaisedButton:
                            text: "transaction_import"
                            on_release: app.transaction_import()
                            pos_hint: {"top": 1}
                    MDBoxLayout:
                        orientation: 'vertical'
                        spacing: "5dp"
                        MDRaisedButton:
                            text: "Test"
            TabbedPanelItem:
                background_color:hex('#151D91')
                text: "WB"

"""


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
            bqtable,
            method,
            fieldname,
            ozon_data_filter_type,
            self.root.ids.start_date.text,
            self.root.ids.end_date.text,
        )

        pass


def update_transaction_orders_by_period(
    bqtable, method, fieldname, ozon_data_filter_type, datefrom, dateto
):
    bqdataset = "OZON"
    bqjsonservicefile = "polar.json"
    configyml = "config_ozon.yml"
    daterange = {
        "datefrom": date.fromisoformat(datefrom),
        "dateto": date.fromisoformat(dateto),
    }

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
