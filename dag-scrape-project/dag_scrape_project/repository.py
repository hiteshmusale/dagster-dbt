from dagster import (
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    fs_io_manager,
    load_assets_from_package_module,
    repository,
    with_resources,
    asset,
    job
)

from dagster._utils import file_relative_path
from dagster_dbt import dbt_cli_resource,load_assets_from_dbt_project

from dag_scrape_project import assets
from dag_scrape_project.assets.scrape_listings import scrape_listings
from dag_scrape_project.assets.scrape_popup_links import scrape_popup_links

import os
import requests
from bs4 import BeautifulSoup

#DBT project path
DBT_PROJECT_DIR = file_relative_path(__file__,"../scrape_project_dbt")
DBT_PROFILE_DIR = file_relative_path(__file__,"../scrape_project_dbt/config")

#Loading DBT models as assets
dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    #DBT_PROFILE_DIR,
    key_prefix="ecom",
)

#Loading dagster assets 
scrape_assets = load_assets_from_package_module(assets)

#Creating dbt_resources from dbt_assets
dbt_resources = with_resources(dbt_assets,{"dbt":dbt_cli_resource.configured(
    {"project_dir":DBT_PROJECT_DIR,
    #"profile_dir":DBT_PROFILE_DIR
    },
    )
},
)

#Defining scrape_listings_job from scrape_listing asset
@job
def scrape_listings_job():
    scrape_listings()

#Defining scrape_popup_links_job from scrape_popup_links asset
@job
def scrape_popup_links_job():
    scrape_popup_links()

#Defining scrape dbt job from scrape models 
scrape_dbt_job=define_asset_job("Common_product_name_job",selection=AssetSelection.groups("scrape_models"))



@repository
def dag_scrape_project():
    return [
        dbt_resources,
        scrape_assets,
        ScheduleDefinition(job=scrape_dbt_job,cron_schedule="25 10 * * *",execution_timezone="Asia/Calcutta"),
        ScheduleDefinition(job=scrape_listings_job,cron_schedule="25 10 * * *",execution_timezone="Asia/Calcutta"),
        ScheduleDefinition(job=scrape_popup_links_job,cron_schedule="25 10 * * *",execution_timezone="Asia/Calcutta")
    
    ]




