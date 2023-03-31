

from dagster import asset
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery

from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
from pprint import pprint

#Configure the BigQuery client
service_account_info = json.load(open("/home/dmytro_fedoru/.dbt/keyfile.json"))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)


#This asset scrapes the popup_links dataset

@asset
def scrape_popup_links():
    url = "https://webscraper.io/test-sites/e-commerce/allinone"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    popup_links = []
    for item in soup.select(".thumbnail"):
        title = item.select(".title")[0].text.strip()
        popup_links.append(title)

    # Load popup_links data into a BigQuery table
    table_id = "dagstertest-382218.TestScrapeDataset.popup_links"

    records = [{"title": title} for title in popup_links]
    errors = client.insert_rows_json(table_id, records)
    #print(errors)
