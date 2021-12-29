import datetime
from tkinter import *
from tkinter import ttk

import bq_method
from dateutil.relativedelta import relativedelta

setting = {}
ws = Tk()
ws.title("GUI data olap")
# ws.geometry('500x500')
ws["bg"] = "gray26"


def verify(bqjsonservicefile="polar.json", bqdataset="DB2019", bqtable="ozon_wb_1c"):
    setting["bqjsonservicefile"] = bqjsonservicefile
    setting["bqdataset"] = bqdataset
    setting["bqtable"] = bqtable

    b1 = ttk.Button(text="Verify last mont")
    b1.bind("<Button-1>", verify_last_month)
    b1.pack()
    b2 = ttk.Button(text="Verify last week")
    b2.bind("<Button-1>", verify_last_week)
    b2.pack()

    ws.mainloop()


def verify_last_month(b):
    print("month")
    filterList = []
    date_start = datetime.date.today() - relativedelta(months=1)
    date_end = datetime.date.today() - relativedelta(days=1)

    unitlist = '("OZON", "WILDBERRIES", "ЯНДЕКС")'
    field = "Unit"
    filterList.append(
        {
            "fieldname": field,
            "operator": "in",
            "value": unitlist,
        }
    )
    filterList.append(
        {
            "fieldname": "date",
            "operator": ">=",
            "value": date_start.strftime("%Y-%m-%d"),
        }
    )
    filterList.append(
        {
            "fieldname": "date",
            "operator": "<=",
            "value": date_end.strftime("%Y-%m-%d"),
        }
    )
    operationlist = ("Продажи WB-OZON-YM ЛК", "Продажи WB-OZON ЛК (old)")
    field = "Operation"
    querytotal = ""
    fieldlist = "date,day,week,month,year,Operation,Unit,Value,0 as ValueOld"
    for oper in operationlist:
        if querytotal != "":
            querytotal = querytotal + " UNION ALL "
            fieldlist = "date,day,week,month,year,Operation,Unit,0,Value"
        newfilter = filterList.copy()
        newfilter.append(
            {
                "fieldname": "Operation",
                "operator": "=",
                "value": oper,
            }
        )

        query = bq_method.get_selectquery_for_table(
            setting["bqjsonservicefile"],
            setting["bqdataset"],
            setting["bqtable"],
            newfilter,
            fieldlist,
        )
        querytotal = querytotal + query
    querytotal = (
        "select date,day,week,month,year,Unit,Sum(Value) Value,Sum(ValueOld) ValueOld from ("
        + querytotal
        + ") as grp Group by date,day,week,month,year,Unit Order by date,Unit"
    )

    resultquery = bq_method.SelectQuery(
        setting["bqjsonservicefile"],
        setting["bqdataset"],
        setting["bqtable"],
        filterList,
        querytotal,
    )
    game_frame = Frame(ws)
    game_frame.pack()
    my_game = ttk.Treeview(game_frame)
    my_game["columns"] = ("Period", "Unit", "Value_LK", "Value_LK_OLD", "DeltaPercent")
    my_game.column("#0", width=0, stretch=NO)
    my_game.column("Period", anchor=CENTER, width=80)
    my_game.column("Unit", anchor=CENTER, width=80)
    my_game.column("Value_LK", anchor=CENTER, width=80)
    my_game.column("Value_LK_OLD", anchor=CENTER, width=80)
    my_game.column("DeltaPercent", anchor=CENTER, width=80)

    my_game.heading("#0", text="", anchor=CENTER)
    my_game.heading("Period", text="Period", anchor=CENTER)
    my_game.heading("Unit", text="Unit", anchor=CENTER)
    my_game.heading("Value_LK", text="Value LK", anchor=E)
    my_game.heading("Value_LK_OLD", text="Value manual", anchor=E)
    my_game.heading("DeltaPercent", text="Delta, %", anchor=E)
    count = 0
    for row in resultquery:
        tag = "normal"
        DeltaPercent = "N/A"
        if row.ValueOld != 0.0:
            DeltaPercent = 100 - row.ValueOld / row.Value * 100
            if DeltaPercent > 10 or DeltaPercent < -10:
                tag = "red"

        my_game.insert(
            parent="",
            index="end",
            iid=count,
            text="",
            values=(row.date, row.Unit, row.Value, row.ValueOld, DeltaPercent),
            tags=tag,
        )
        count = count + 1
    my_game.tag_configure("red", background="red")
    my_game.pack()
    ws.mainloop()
    pass


def verify_last_week(b):
    pass


if __name__ == "__main__":
    verify()
