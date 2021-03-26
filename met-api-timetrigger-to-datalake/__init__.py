import datetime
import logging
import json
import pandas as pd
from pandas.io.json import json_normalize
import requests
from time import sleep

from azure.storage.blob import ContainerClient
from azure.keyvault.secrets import *
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:

    # Initialize the Credentials.
    default_credential = DefaultAzureCredential()
    logging.info("=== Initialized credentials")

    # Create a Secret Client, so we can grab our Connection String.
    secret_client = SecretClient(
        vault_url='https://met-api-kv.vault.azure.net/',
        credential=default_credential
    )
    logging.info("=== Initialized secret credentials")

     # Grab the Blob Connection String, from our Azure Key Vault.
    blob_conn_string = secret_client.get_secret(
        name='metapistorageaccountkey'
    )
    logging.info("=== Retrieved storage connection string")


     # Connect to the Container.
    container_client = ContainerClient.from_connection_string(
        conn_str=blob_conn_string.value,
        container_name='metapidata'
    )
    logging.info("=== Connected to container")

    full_date = datetime.datetime.now()
    date_hour_minute_long = str(full_date)+'-'+str(full_date.hour)+':'+str(full_date.minute)
    date_hour_minute = date_hour_minute_long[0:16]

    headers = {"User-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"}
    lat_lon = [("Kronstad", 60.37, 5.34), ("Flesland", 60.29, 5.23)]
    date = datetime.date.today()
    for idx, val in enumerate(lat_lon):
        loc = val[0]
        lat = val[1]
        lon = val[2]
        url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={lat}&lon={lon}"
        response = requests.get(url=url, headers=headers)
        json_raw = response.json()
        json_timeseries = json_raw['properties']['timeseries']
        json_timeseries_flattened = pd.json_normalize(json_timeseries)
        json_timeseries_flattened['time_created'] = datetime.datetime.now()
        filename = f"{loc}/{date}/{date_hour_minute}.csv"
        csv_timeseries = json_timeseries_flattened.to_csv(index=False)

        container_client.upload_blob(
            name=filename,
            data=csv_timeseries,
            blob_type="BlockBlob")
        sleep(1)
    
    logging.info("=== Success")
