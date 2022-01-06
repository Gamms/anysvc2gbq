# type: ignore[attr-defined]
import datetime
from enum import Enum

import typer
import yaml
from client1c import upload_from_1c
from dateutil import parser
from dateutil.relativedelta import relativedelta
from loguru import logger
from rich.console import Console
from verifydata import verify
from xmldatafeed import xmldatafeed

folderLog = "log/"
app = typer.Typer(
    name="xmldatafeed2gbq",
    help="import from xmldatafeed.com/1c to Google big query",
    add_completion=False,
)
console = Console()


class periodOption(Enum):
    last_day = 1
    last_2day = 2
    last_week = 3
    last_month = 4
    last_quarter = 5
    manual = 6


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
    period_option: periodOption = 1,
    datestock_start_str: str = "",
    datestock_end_str: str = "",
) -> None:
    if period_option == periodOption.last_day:
        datestock_start = datetime.datetime.now()
        datestock_end = datetime.datetime.now()
    elif period_option == periodOption.last_2day:
        datestock_end = datetime.datetime.now()
        datestock_start = datetime.datetime.now() - relativedelta(days=1)
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

    with open("client1C_config.yml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    upload_from_1c(
        config, bqjsonservicefile, bqdataset, bqtable, datestock_start, datestock_end
    )


@logger.catch
@app.command()
def verifydata(
    bqjsonservicefile: str, bqdataset: str = "DB2019", bqtable: str = "wb_ozon_1c"
) -> None:

    verify(bqjsonservicefile, bqdataset, bqtable)

@logger.catch
@app.command()
def gui(
    bqjsonservicefile: str='polar.json', bqdataset: str = "DB2019", bqtable: str = "wb_ozon_1c"
) -> None:

    verify(bqjsonservicefile, bqdataset, bqtable)

if __name__ == "__main__":
    app()
