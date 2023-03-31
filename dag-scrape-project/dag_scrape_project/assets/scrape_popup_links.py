
from dagster import asset
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery

#Configure the BigQuery client
client = bigquery.Client()
#dagstertest-382218.TestScrapeDataset.listings

@asset
def scrape_popup_links():
    url = "https://webscraper.io/test-sites/e-commerce/allinone"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    popup_links = []
    for item in soup.select(".thumbnail"):
        title = item.select(".title")[0].text.strip()
        popup_links.append(title)

    # Load listings data into a BigQuery table
    #dataset_id = "your_dataset_id"
    #table_id = f"{dataset_id}.listings"
    table_id = "dagstertest-382218.TestScrapeDataset.popup_links"

    records = [{"title": title} for title in popup_links]
    errors = client.insert_rows_json(table_id, records)
    print(errors)
    # if errors == []:
    #     context.log.info("Loaded popup_links data into BigQuery.")
    # else:
    #     context.log.error(f"Failed to load listings data: {errors}")
