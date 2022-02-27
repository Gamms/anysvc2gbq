# type: ignore[attr-defined]
import datetime
from enum import Enum

import transfer_method
import typer
import yaml
from client1c import export_item_to_bq, upload_from_1c
from common_type import periodOption
from dateutil import parser
from dateutil.relativedelta import relativedelta
from loguru import logger
from ozon_method import OzonDataFilterType
from rich.console import Console
from verifydata import verify
from xmldatafeed import xmldatafeed
from transfer_method import wb_export

folderLog = "log/"
app = typer.Typer(
    name="xmldatafeed2gbq",
    help="import from xmldatafeed.com/1c to Google big query",
    add_completion=False,
)
console = Console()


class periodOption(str, Enum):
    last_day = "last_day"
    last_2day = "last_2day"
    last_week = "last_week"
    last_month = "last_month"
    last_quarter = "last_quarter"
    manual = "manual"
    changes = "changes"


class ozonOperation(str, Enum):
    orders = "orders"
    transaction = "transaction"
    fbo_orders = "fbo_orders"

class wbOperation(str, Enum):
    sales = "sales"
    orders = "orders"
    report = "report"
    stock = "stock"


def version_callback(print_version: bool) -> None:
    """Print the version of the package."""
    if print_version:
        console.print(f"[yellow]xmldatafeed2gbq[/] version: [bold blue]{version}[/]")
        raise typer.Exit()


@logger.catch
@app.command()
def uploadfromxmldatafeed(
    downloadpath: str, user: str, password: str, bqjsonservicefile: str, bqdataset: str
) -> None:
    logger_init(downloadpath)
    with open("xml_feed_file_config.yml") as f:
        config = yaml.safe_load(f)

    xmldatafeed(bqdataset, bqjsonservicefile, config, downloadpath, password, user)


def logger_init(downloadpath):
    logger.add(
        downloadpath + folderLog + "infolog_{time}.log",
        rotation="12:00",
        compression=zip,
        retention="14 days",
        level="INFO",
    )
    logger.add(
        downloadpath + folderLog + "errorlog_{time}.log",
        rotation="100 MB",
        compression=zip,
        retention="100 days",
        level="ERROR",
    )


@logger.catch
@app.command()
def uploadfrom1C(
    bqjsonservicefile: str,
    bqdataset: str = "DB1C",
    bqtable: str = "tableentry1c",
    period_option: periodOption = periodOption.last_2day,
    datestock_start_str: str = "",
    datestock_end_str: str = "",
) -> None:
    daterange = fill_daterange_from_option(
        datestock_end_str, datestock_start_str, period_option
    )
    with open("client1C_config.yml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    upload_from_1c(
        config,
        bqjsonservicefile,
        bqdataset,
        bqtable,
        daterange["datefrom"],
        daterange["dateto"],
    )


def fill_daterange_from_option(datestock_end_str, datestock_start_str, period_option):
    if period_option == periodOption.last_day:
        datestock_start = datetime.datetime.now()
        datestock_end = datetime.datetime.now()
    elif period_option == periodOption.last_2day:
        datestock_end = datetime.datetime.now()
        datestock_start = datetime.datetime.now() - relativedelta(days=1, second=1)
    elif period_option == periodOption.last_week:
        datestock_end = datetime.datetime.now()
        datestock_start = datetime.datetime.now() - relativedelta(weeks=1)
    elif period_option == periodOption.last_month:
        datestock_end = datetime.datetime.now()
        datestock_start = datetime.datetime.now() - relativedelta(months=1)
    elif period_option == periodOption.last_quarter:
        datestock_end = datetime.datetime.now()
        datestock_start = datetime.datetime.now() - relativedelta(months=3)
    elif period_option == periodOption.manual:
        datestock_end = parser.parse(datestock_end_str)
        datestock_start = parser.parse(datestock_start_str)
    daterange = {"datefrom": datestock_start, "dateto": datestock_end}
    return daterange


@logger.catch
@app.command()
def verifydata(
    bqjsonservicefile: str, bqdataset: str = "DB2019", bqtable: str = "wb_ozon_1c"
) -> None:

    verify(bqjsonservicefile, bqdataset, bqtable)


@logger.catch
@app.command()
def gui(
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "DB2019",
    bqtable: str = "wb_ozon_1c",
) -> None:
    verify(bqjsonservicefile, bqdataset, bqtable)


@logger.catch
@app.command()
def upload_from_ozon2bq(
    operation: ozonOperation,
    bqtable: str,
    date_filter_field: OzonDataFilterType = OzonDataFilterType.updated_at,
    period_option: periodOption = periodOption.last_2day,
    datestock_start_str: str = "",
    datestock_end_str: str = "",
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "OZON",
    configyml: str = "config_ozon.yml",
) -> None:
    daterange = fill_daterange_from_option(
        datestock_end_str, datestock_start_str, period_option
    )
    if operation == ozonOperation.transaction:
        method = "transactionv3"
        date_filter_field = OzonDataFilterType.date
        fieldname = "operation_date"
        transfer_method.transfer_orders_transaction_ozon2bq_in_the_period(
            daterange,
            bqdataset,
            bqjsonservicefile,
            bqtable,
            configyml,
            fieldname,
            method,
            date_filter_field,
        )
    elif operation == ozonOperation.fbo_orders:
        method = "fbo_orders"
        fieldname = "created_at"
        date_filter_field = OzonDataFilterType.since
        transfer_method.transfer_orders_transaction_ozon2bq_in_the_period(
            daterange,
            bqdataset,
            bqjsonservicefile,
            bqtable,
            configyml,
            fieldname,
            method,
            date_filter_field,
        )
    else:
        fieldname = "created_at"
        method = "orders"
        if date_filter_field == OzonDataFilterType.updated_at:
            transfer_method.export_orders_from_ozon2bq_updated_in_the_period(
                daterange, bqdataset, bqjsonservicefile, bqtable, configyml, method
            )
        else:
            transfer_method.transfer_orders_transaction_ozon2bq_in_the_period(
                daterange,
                bqdataset,
                bqjsonservicefile,
                bqtable,
                configyml,
                fieldname,
                method,
                date_filter_field,
            )


@logger.catch
@app.command()
def uploadfrom1C_item(
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "DB2019",
    bqtable: str = "item_ref1C",
    fileconfig1c: str = "client1C_config.yml",
) -> None:
    export_item_to_bq(fileconfig1c, bqjsonservicefile, bqdataset, bqtable)

@logger.catch
@app.command()
def upload_from_ozon2bq(
    operation: wbOperation,
    bqtable: str,
    period_option: periodOption = periodOption.changes,
    datestock_start_str: str = "",
    datestock_end_str: str = "",
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "wb",
) -> None:
    if period_option==periodOption.changes:
        option = "changes"
        datefromiso=''
        datefromiso = ''
    else:
        daterange = fill_daterange_from_option(
            datestock_end_str, datestock_start_str, period_option
        )
        option='byPeriod'
        datefromiso = daterange["datefrom"].isoformat()
        datetoiso = daterange["dateto"].isoformat()

    if operation==wbOperation.orders:
        method = "orders"
    elif operation==wbOperation.sales:
        method = "sales"

    wb_export(method, bqtable, option,datefromiso,datetoiso,jsonkey=bqjsonservicefile,bqdataset=bqdataset)


if __name__ == "__main__":
    app()
