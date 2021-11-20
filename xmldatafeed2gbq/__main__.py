# type: ignore[attr-defined]
from typing import Optional

import csv
import datetime
import os
import shutil
import xml.etree.ElementTree as ET
import zipfile
from enum import Enum
from random import choice

import bq_method
import requests
import typer
from loguru import logger
from rich.console import Console

from xmldatafeed2gbq import version
from xmldatafeed2gbq.example import hello

folderUnzipped = "unzipped/"
folderUnzippedError = "unzippedERROR/"
folderComplete = "complete/"
folderDownloaded = "downloaded/"
folderDownloadedError = "downloadedERROR/"
folderError = "error/"
folderLog = "log/"
app = typer.Typer(
    name="xmldatafeed2gbq",
    help="import from xmldatafeed.com to Google big query",
    add_completion=False,
)
console = Console()


def returnContentsListByType(typefile: str):
    if typefile == "ozon":
        list = [
            "Date",
            "Brend",
            "Seller",
            "Name_pr",
            "Code",
            "Position",
            "Page_n",
            "Old_pr",
            "Price",
        ]
    elif typefile == "ozon_char":
        list = [
            "Date",
            "Brend",
            "Seller",
            "Name_pr",
            "Code",
            "Sup_art",
            "Position",
            "Page_n",
            "Old_pr",
            "Price",
            "Size",
            "Type",
            "Cover_fb",
            "Form",
            "Pattern",
            "Color",
        ]
    elif typefile == "wb":
        list = [
            "Date",
            "Brend",
            "Seller",
            "Name_pr",
            "Code",
            "Position",
            "Page_n",
            "Old_pr",
            "Price",
            "Cover_fb",
            "Color",
            "Discount",
            "Rating",
            "Reviwes",
        ]
    elif typefile == "ym":
        list = [
            "Date",
            "Brend",
            "Name_pr",
            "Code",
            "Position",
            "Page_n",
            "Old_pr",
            "Price",
            "Shipping",
        ]

    else:
        list = []
    return list


def getallfilesfromxmlfeed(session, url, path, path_for_download, local_file_list):
    res = session.request("PROPFIND", url + path)
    root = ET.fromstring(res.content)
    elements = root.findall("{DAV:}response/{DAV:}href")
    for element in elements:
        if element.text == path:
            continue
        pathlocal = element.text
        if pathlocal.find("csv.zip") > -1:
            filename = os.path.basename(pathlocal)
            if filename in local_file_list:
                continue
            resfile = session.get(url + pathlocal)
            if resfile.status_code == 200:
                img = resfile.content
                print(path_for_download + filename)

                with open(path_for_download + filename, "wb") as f:
                    f.write(img)

        else:
            getallfilesfromxmlfeed(
                session, url, pathlocal, path_for_download, local_file_list
            )


def unzipFile(filename, path_for_download):
    with zipfile.ZipFile(path_for_download + filename, "r") as zip_ref:
        for fileinzip in zip_ref.filelist:
            fileinzipname = fileinzip.filename
            zip_ref.extract(fileinzipname, path_for_download + folderUnzipped)


def readCsv2Js(fileinzipname, typefile, path_for_download):
    jslist = []
    with open(path_for_download + fileinzipname) as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        rowcount = 0
        for row in reader:
            rowcount += 1
            if rowcount <= 2:
                continue
            dict = {}
            contentlist = returnContentsListByType(typefile)

            for index, value in enumerate(contentlist):
                if value == "Date":
                    dict[value] = (
                        datetime.datetime.strptime(row[index], "%d.%m.%Y")
                        .date()
                        .isoformat()
                    )
                else:
                    dict[value] = row[index]

            jslist.append(dict)
    return jslist


def version_callback(print_version: bool) -> None:
    """Print the version of the package."""
    if print_version:
        console.print(f"[yellow]xmldatafeed2gbq[/] version: [bold blue]{version}[/]")
        raise typer.Exit()


def get_list_local_file(downloadpath, list_folder):
    result = []
    for el in list_folder:
        content = os.listdir(downloadpath + el)
        for file in content:
            if os.path.isfile(os.path.join(downloadpath + el, file)) and file.endswith(
                ".zip"
            ):
                result.append(file)
    return result


@logger.catch
@app.command()
def main(
    downloadpath: str, user: str, password: str, bqjsonservicefile: str, bqdataset: str
) -> None:
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
    auth = requests.auth.HTTPBasicAuth(user, password)
    basicurl = "https://ru.xmldatafeed.com"
    basicpath = "/remote.php/dav/files/ankuznet@gmail.com/"
    session = requests.Session()
    session.auth = auth
    list_local_file = get_list_local_file(
        downloadpath, [folderDownloaded, folderDownloadedError]
    )

    getallfilesfromxmlfeed(session, basicurl, basicpath, downloadpath, list_local_file)

    unzipFileInFolder(downloadpath)

    content = os.listdir(downloadpath + folderUnzipped)
    for file in content:
        if os.path.isfile(
            os.path.join(downloadpath + folderUnzipped, file)
        ) and file.endswith(".csv"):
            typefile = ""
            if file.find("wildberries") > -1:
                typefile = "wb"
                bqtableid = "WB_pars_auto"
            elif file.find("ozonbagchaircategories_characteristics") > -1:
                typefile = "ozon_char"
                bqtableid = "Ozon_pars_auto"
            elif file.find("ozon") > -1:
                typefile = "ozon"
                bqtableid = "Ozon_pars_auto"
            elif file.find("yamarket") > -1:
                typefile = "ym"
                bqtableid = "Ym_pars_auto"
            else:
                typefile = "error"
                logger.error(f"Неизвестный тип файла:{file}")
                return
            try:
                logger.info(f"Читаем {file}")
                jslist = readCsv2Js(file, typefile, downloadpath + folderUnzipped)
                bq_method.export_js_to_bq(
                    jslist, bqtableid, bqjsonservicefile, bqdataset, logger
                )
                shutil.move(
                    downloadpath + folderUnzipped + file,
                    downloadpath + folderComplete + file,
                )
            except Exception as e:
                logger.exception("Ошибка чтения csv")
                shutil.move(
                    downloadpath + folderUnzipped + file,
                    downloadpath + folderError + file,
                )


def unzipFileInFolder(downloadpath):
    content = os.listdir(downloadpath)
    for file in content:
        if os.path.isfile(os.path.join(downloadpath, file)) and file.endswith(".zip"):
            try:
                unzipFile(file, downloadpath)
                shutil.move(downloadpath + file, downloadpath + folderDownloaded + file)
            except Exception as e:
                logger.exception("Ошибка распаковки")
                shutil.move(
                    downloadpath + file, downloadpath + folderDownloadedError + file
                )


if __name__ == "__main__":
    app()
