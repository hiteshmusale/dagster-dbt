# import requests
# from bs4 import BeautifulSoup
# from google.cloud import bigquery
# from dagster import solid, pipeline, repository, Field, InputDefinition, OutputDefinition, Output
# import dagster_dbt

# # Configure the BigQuery client
# client = bigquery.Client()

# @solid
# def scrape_listings(context):
#     url = "https://webscraper.io/test-sites/e-commerce/allinone"
#     response = requests.get(url)
#     soup = BeautifulSoup(response.text, "html.parser")

#     listings = []
#     for item in soup.select(".thumbnail"):
#         title = item.select(".title")[0].text.strip()
#         listings.append(title)

#     return listings


# @solid
# def scrape_popup_links(context):
#     url = "https://webscraper.io/test-sites/e-commerce/allinone-popup-links"
#     response = requests.get(url)
#     soup = BeautifulSoup(response.text, "html.parser")

#     popup_links = []
#     for item in soup.select(".thumbnail"):
#         title = item.select(".title")[0].text.strip()
#         popup_links.append(title)

#     return popup_links


# @solid(input_defs=[InputDefinition("listings", list)], output_defs=[OutputDefinition(list)])
# def load_listings_to_bigquery(context, listings):
#     # Load listings data into a BigQuery table
#     dataset_id = "your_dataset_id"
#     table_id = f"{dataset_id}.listings"

#     records = [{"title": title} for title in listings]
#     errors = client.insert_rows_json(table_id, records)
#     if errors == []:
#         context.log.info("Loaded listings data into BigQuery.")
#     else:
#         context.log.error(f"Failed to load listings data: {errors}")

#     return listings


# @solid(input_defs=[InputDefinition("popup_links", list)], output_defs=[OutputDefinition(list)])
# def load_popup_links_to_bigquery(context, popup_links):
#     # Load popup links data into a BigQuery table
#     dataset_id = "your_dataset_id"
#     table_id = f"{dataset_id}.popup_links"

#     records = [{"title": title} for title in popup_links]
#     errors = client.insert_rows_json(table_id, records)
#     if errors == []:
#         context.log.info("Loaded popup links data into BigQuery.")
#     else:
#         context.log.error(f"Failed to load popup links data: {errors}")

#     return popup_links


# @solid(config_schema={"dbt_project_path": Field(str), "target": Field(str, is_required=False, default_value="dev")})
# def run_dbt_sql(context):
#     result = dagster_dbt.run_dbt(
#         context,
#         project_dir=context.solid_config["dbt_project_path"],
#         profiles_dir=None,
#         target=context.solid_config["target"],
#         models=None,
#         exclude=None,
#         selector=None,
#     )
#     return result


# @pipeline
# def ecommerce_pipeline():
#     listings = scrape_listings()
#     popup_links = scrape_popup_links()

# if _name_ == "_main_":
#     from dagster import execute_pipeline

#     execute_pipeline(ecommerce_pipeline)