# type: ignore[attr-defined]
import datetime
import functools
from enum import Enum

import method_telegram
import transfer_method
import typer
import yaml
from client1c import (
    export_documents_commission_report_from_1c2bq,
    export_documents_of_service_receipt_from_1c2bq,
    export_item_to_bq,
    export_order_status_history_from_1c2bq,
    export_price_to_bq,
    upload_from_1c,
)
from common_type import periodOption
from dateutil import parser
from dateutil.relativedelta import relativedelta
from loguru import logger
from ozon_method import OzonDataFilterType
from rich.console import Console
from verifydata import verify
from xmldatafeed import xmldatafeed


def logger_wraps(*, entry=True, exit=True, level="DEBUG"):
    def wrapper(func):
        name = func.__name__

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            logger_ = logger.opt(depth=1)
            if entry:
                logger_.log(
                    level, "Entering '{}' (args={}, kwargs={})", name, args, kwargs
                )
            result = func(*args, **kwargs)
            if exit:
                logger_.log(level, "Exiting '{}' (result={})", name, result)
            return result

        return wrapped

    return wrapper


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


class yandexOperation(str, Enum):
    orders = "orders"


class ozonOperation(str, Enum):
    orders = "orders"
    transaction = "transaction"
    fbo_orders = "fbo_orders"
    stocks = "stocks"


class wbOperation(str, Enum):
    sales = "sales"
    orders = "orders"
    report = "report"
    stock = "stock"
    orders_v2 = "orders_v2"
    invoice = "invoice"


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
    elif operation == ozonOperation.stocks:
        transfer_method.export_stocks_from_ozon2bq(
            bqdataset,
            bqjsonservicefile,
            bqtable,
            configyml,
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
def uploadfrom1C_price(
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "DB2019",
    bqtable: str = "item_price1C",
    fileconfig1c: str = "client1C_config.yml",
) -> None:
    export_price_to_bq(fileconfig1c, bqjsonservicefile, bqdataset, bqtable)


@logger.catch
@app.command()
def uploadfrom1C_orders_history_status(
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "RefTable",
    bqtable: str = "orders_history_status1C",
    fileconfig1c: str = "client1C_config.yml",
) -> None:
    export_order_status_history_from_1c2bq(
        fileconfig1c, bqjsonservicefile, bqdataset, bqtable
    )


@logger.catch
@app.command()
def upload_from_wb2bq(
    operation: wbOperation,
    bqtable: str,
    period_option: periodOption = periodOption.changes,
    datestock_start_str: str = "",
    datestock_end_str: str = "",
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "wb",
) -> None:
    if period_option == periodOption.changes:
        option = "changes"
        datetoiso = ""
        datefromiso = ""
    else:
        daterange = fill_daterange_from_option(
            datestock_end_str, datestock_start_str, period_option
        )
        option = "byPeriod"
        datefromiso = daterange["datefrom"].isoformat()
        datetoiso = daterange["dateto"].isoformat()

    if operation == wbOperation.orders:
        method = "orders"
    elif operation == wbOperation.sales:
        method = "sales"
    elif operation == wbOperation.report:
        method = "reportsale"
    elif operation == wbOperation.orders_v2:
        method = "ordersv2"
    elif operation == wbOperation.orders_v2:
        method = "invoice_v1"
    else:
        raise "Не настроена выгрузка для " + operation

    transfer_method.wb_export(
        method,
        bqtable,
        option,
        datefromstr=datefromiso,
        datetostr=datetoiso,
        jsonkey=bqjsonservicefile,
        datasetid=bqdataset,
    )


@logger.catch
@app.command()
def upload_from_yandex2bq(
    operation: yandexOperation,
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "YM",
    bqtable: str = "orders",
    fileconfigyandex: str = "config_yandex.yml",
):
    if operation == yandexOperation.orders:
        transfer_method.export_orders_from_ym2bq(
            bqdataset, bqjsonservicefile, bqtable, fileconfigyandex
        )
    else:
        logger.error(f"Operation {operation} dont recognized!")

    pass


@logger.catch
@app.command()
def upload_stocks_from_1c2ym(
    fileconfig1c: str = "client1C_config.yml",
    fileconfigyandex: str = "config_yandex.yml",
):
    transfer_method.export_stocks_from_1c2ym(fileconfig1c, fileconfigyandex)


@logger.catch
@app.command()
def upload_document_service_from_1c2bq(
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "DB1C",
    bqtable: str = "ReceiptOfServices",
    fileconfig1c: str = "client1C_config.yml",
    period_option: periodOption = periodOption.manual,
    datestock_start_str: str = "",
    datestock_end_str: str = "",
) -> None:
    daterange = fill_daterange_from_option(
        datestock_end_str, datestock_start_str, period_option
    )
    export_documents_of_service_receipt_from_1c2bq(
        fileconfig1c,
        bqjsonservicefile,
        bqdataset,
        bqtable,
        daterange["datefrom"],
        daterange["dateto"],
    )


@logger.catch
@app.command()
def upload_document_commissionreport(
    bqjsonservicefile: str = "polar.json",
    bqdataset: str = "DB1C",
    bqtable: str = "CommissionReport",
    fileconfig1c: str = "client1C_config.yml",
) -> None:
    export_documents_commission_report_from_1c2bq(
        fileconfig1c,
        bqjsonservicefile,
        bqdataset,
        bqtable,
    )


if __name__ == "__main__":
    tg_handler = method_telegram.get_loguru_telegramm_notification_handler(
        logger, "-1001572341087", "2028570019:AAEhd5gfY6qxZRmJZfymO82xSO4E-VuMXjU"
    )
    if tg_handler != None:
        logger.add(tg_handler, level="ERROR")
    app()
