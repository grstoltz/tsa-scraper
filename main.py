
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from google.cloud import bigquery
import pyarrow

def hello_world(request):

    url="https://www.tsa.gov/coronavirus/passenger-throughput"

    # Make a GET request to fetch the raw HTML content
    html_content = requests.get(url).text

    # Parse the html content
    soup = BeautifulSoup(html_content, "lxml")
    table = soup.find('tbody')
    rows = table.find_all('tr')
    data = []
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)

    df = pd.DataFrame(data, columns=['date', 'throughput_2021', 'throughput_2020', 'throughput_2019'])

    df["throughput_2021"] = df["throughput_2021"].str.replace(',', '').replace('', 0).astype(float)
    df["throughput_2020"] = df["throughput_2020"].str.replace(',', '').replace('', 0).astype(float)
    df["throughput_2019"] = df["throughput_2019"].str.replace(',', '').replace('', 0).astype(float)


    df['2019_7_day_avg'] = df['throughput_2019'].rolling(7, min_periods=1).mean()[::-1].round(1)  
    df['2020_7_day_avg'] = df['throughput_2020'].rolling(7, min_periods=1).mean()[::-1].round(1)
    df['2021_7_day_avg'] = df['throughput_2021'].rolling(7, min_periods=1).mean()[::-1].round(1)

    client = bigquery.Client()
    dataset_id = 'tsa-scraper:tsa'
    table_name= 'daily_throughput'

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"
    #job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    load_job = client.load_table_from_dataframe(df, dataset_ref.table(table_name), job_config=job_config)
    print("Starting job {}".format(load_job))