import datetime
import os

import loguru
import pytz
from google.cloud import bigquery as bq
from google.oauth2 import service_account


def export_js_to_bq(js, tableid, key_path, dataset_id, logger, fields_list):
    table_id = tableid
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    bigquery_client = bq.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    job_config = bq.LoadJobConfig()
    # Schema autodetection enabled
    schema = get_schema_bqtable_from_list(fields_list)
    if schema != []:
        job_config.schema = schema
    else:
        job_config.autodetect = True
    # Skipping first row which correspnds to the field names
    # Format of the data in GCS
    job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema_update_options = bq.SchemaUpdateOption.ALLOW_FIELD_ADDITION

    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    logger.info(
        f"Starting uploading data. Record count {len(js)} into the BQ table {table_id}"
    )
    job = bigquery_client.load_table_from_json(js, table_ref, job_config=job_config)
    # load_job = bigquery_client.load_table_from_file(csvfile,table_ref,job_config)
    try:
        job.result()  # Waits for table load to complete.
        logger.info(f"end uploading data.")
    except Exception as e:
        print("Unexpected error:", e)
        raise


def CreateDataSet(dataset_id, key_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    bigquery_client = bq.Client()
    dataset = bq.Dataset(f"{credentials.project_id}.{dataset_id}")

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = bigquery_client.create_dataset(dataset)  # Make an API request.
    print(f"Created dataset {bigquery_client.project}.{dataset.dataset_id}")


def DeleteTable(table_id, dataset_id, key_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    bigquery_client = bq.Client()
    # dataset = bq.Dataset('{}.{}'.format(credentials.project_id,dataset_id))
    fulltableid = f"{credentials.project_id}.{dataset_id}.{table_id}"
    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = bigquery_client.delete_table(fulltableid)  # Make an API request.
    print(f"Delete table {fulltableid}")


def GetMaxRecord(table_id, dataset_id, key_path, wb_id, field):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    fulltableid = f"{credentials.project_id}.{dataset_id}.{table_id}"
    bigquery_client = bq.Client()
    try:

        query = f'SELECT Max(`{field}`) as MaxlastChangeDate  FROM `{fulltableid}` where wb_id="{wb_id}"'
        job_query = bigquery_client.query(query, project=credentials.project_id)
        results = job_query.result()
        maxdate = datetime.date(1, 1, 1)
        for row in results:
            maxdate = row.MaxlastChangeDate
            break
    except Exception as e:
        maxdate = datetime.datetime(1, 1, 1)
    if maxdate == None:
        maxdate = datetime.datetime(1, 1, 1)
    maxdate = maxdate.replace(tzinfo=pytz.UTC)
    return maxdate


def DeleteOldReport(datefrom, dateto, dataset_id, key_path, fieldname, table_id, wb_id):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    fulltableid = f"{credentials.project_id}.{dataset_id}.{table_id}"
    bigquery_client = bq.Client()
    try:
        query = f'Delete FROM `{fulltableid}` where {fieldname}>="{datefrom.strftime("%Y-%m-%d")}" and {fieldname} <"{dateto.strftime("%Y-%m-%d")}" and wb_id="{wb_id}"'
        job_query = bigquery_client.query(query, project=credentials.project_id)
        results = job_query.result()
    except Exception as e:
        print(e)
    return


def SelectQuery(key_path, dataset_id, table_id):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    fulltableid = f"{credentials.project_id}.{dataset_id}.{table_id}"
    bigquery_client = bq.Client()
    try:
        query = f"Select * FROM `{fulltableid}` limit 100"
        job_query = bigquery_client.query(query, project=credentials.project_id)
        results = job_query.result()
    except Exception as e:
        print(e)
    return results


def DeleteRowFromTable(table_id, dataset_id, key_path, filtersList: list):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    fulltableid = f"{credentials.project_id}.{dataset_id}.{table_id}"
    bigquery_client = bq.Client()
    results = 0
    try:
        query = f"Delete FROM `{fulltableid}` where True"
        for elFilter in filtersList:
            if type(elFilter["value"]) is str:
                query = (
                    query
                    + f' and {elFilter["fieldname"]}{elFilter["operator"]}"{elFilter["value"]}"'
                )
            else:
                query = (
                    query
                    + f" and {elFilter['fieldname']}{elFilter['operator']}{elFilter['value']}"
                )

        job_query = bigquery_client.query(query, project=credentials.project_id)
        results = job_query.result()
    except Exception as e:
        print(e)
    return results


def get_schema_bqtable_from_list(fields_list):
    schema = []
    fields_dict = {}
    for element in fields_list:
        for field, type_field in element.items():
            fields_dict[field] = type_field
    schema = get_schema_field_from_dict(fields_dict)
    return schema


def get_schema_field_from_dict(fields: dict) -> list:
    schema = []
    for name, type in fields.items():
        schema.append(bq.SchemaField(name, type))
    return schema
