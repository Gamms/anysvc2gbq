# type: ignore[attr-defined]

import typer
import yaml
from client1c import upload_from_1c
from loguru import logger
from rich.console import Console
from xmldatafeed import xmldatafeed

folderLog = "log/"
app = typer.Typer(
    name="xmldatafeed2gbq",
    help="import from xmldatafeed.com/1c to Google big query",
    add_completion=False,
)
console = Console()


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
    bqjsonservicefile: str, bqdataset: str = "DB1C", bqtable: str = "tableentry1c"
) -> None:

    with open("client1C_config.yml") as f:
        config = yaml.safe_load(f)
    upload_from_1c(config, bqjsonservicefile, bqdataset, bqtable)


if __name__ == "__main__":
    app()
