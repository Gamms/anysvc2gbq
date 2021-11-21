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
import google.api_core.exceptions
import requests
import typer
import yaml
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


def readCsv2Js(fileinzipname, csvfields, path_for_download):
    jslist = []
    with open(path_for_download + fileinzipname) as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        rowcount = 0
        for row in reader:
            rowcount += 1
            if rowcount <= 2:
                continue
            dict = {}

            for index, name_type in enumerate(csvfields):

                for value_name, type_value in name_type.items():
                    value = row[index]
                    if value_name in ["Code", "Rating"]:
                        value = value.replace("=", "").replace(
                            '"', ""
                        )  # у яндекса корявый разбор нужно убрать = и " из текста

                    if type_value == "DATE":
                        value = (
                            datetime.datetime.strptime(value, "%d.%m.%Y")
                            .date()
                            .isoformat()
                        )
                    elif type_value == "INTEGER":
                        if value == "":
                            value = 0
                        value = int(value)
                    elif type_value == "FLOAT":
                        if value == "":
                            value = 0.0
                        value = float(value)
                    elif type_value == "STRING":
                        value = str(value)
                    dict[value_name] = value

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
    with open("xml_feed_file_config.yml") as f:
        config = yaml.safe_load(f)

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

    csv_processing(bqdataset, bqjsonservicefile, downloadpath, config)


def csv_processing(bqdataset, bqjsonservicefile, downloadpath, config):
    content = os.listdir(downloadpath + folderUnzipped)
    for file in content:
        if os.path.isfile(
            os.path.join(downloadpath + folderUnzipped, file)
        ) and file.endswith(".csv"):
            for words in config["word_in_files"]:
                if file.find(words["words"]["word"]) > -1:
                    bqtableid = words["words"]["bqtableid"]
                    csvfields = words["words"]["csvfields"]
                    break
            else:
                logger.error(f"Неизвестный тип файла:{file}. Пропускаем")
                return
            try:
                logger.info(f"Читаем {file}")
                jslist = readCsv2Js(file, csvfields, downloadpath + folderUnzipped)
                if len(jslist) == 0:
                    logger.info(f"Файл пустой:{file}")
                else:
                    bq_method.export_js_to_bq(
                        jslist,
                        bqtableid,
                        bqjsonservicefile,
                        bqdataset,
                        logger,
                        csvfields,
                    )
                shutil.move(
                    downloadpath + folderUnzipped + file,
                    downloadpath + folderComplete + file,
                )
            except google.api_core.exceptions.BadRequest as e:
                logger.exception("Ошибка выгрузки csv в GBQ")
                shutil.move(
                    downloadpath + folderUnzipped + file,
                    downloadpath + folderError + file,
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
