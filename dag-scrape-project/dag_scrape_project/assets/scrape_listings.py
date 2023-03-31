from dagster import asset
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery

#Configure the BigQuery client
client = bigquery.Client()

#This asset scrapes the listings dataset

@asset
def scrape_listings():
    url = "https://webscraper.io/test-sites/e-commerce/allinone"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    listings = []
    for item in soup.select(".thumbnail"):
        title = item.select(".title")[0].text.strip()
        listings.append(title)

    # Load listings data into a BigQuery table
    table_id = "dagstertest-382218.TestScrapeDataset.listings"

    records = [{"title": title} for title in listings]
    errors = client.insert_rows_json(table_id, records)

