import pandas as pd 
import numpy as np 
import requests
from bs4 import BeautifulSoup
import sqlite3
import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_billion']
target_file = "global-gdp-etl-pipeline/Countries_By_GDP.csv"
sql_conn = sqlite3.connect('global-gdp-etl-pipeline/World_Economies.db')
table_name = 'Countries_by_GDP'


def extract(url, table_attribs):
    df = pd.DataFrame(columns = ['Country', 'GDP_USD_million'])
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('tbody')
    target_table = tables[2]
    rows = target_table.find_all('tr')[:50]

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '-' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0], "GDP_USD_million": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    df = df[df["GDP_USD_million"] != "—"]
    df["GDP_USD_million"] = df["GDP_USD_million"].str.replace(",", "", regex=False).astype(int)
    df["GDP_USD_million"] = df["GDP_USD_million"] / 1000
    df["GDP_USD_million"] = np.round(df["GDP_USD_million"], 2)

    df.rename(columns={"GDP_USD_million": "GDP_USD_billions"}, inplace=True)

    return df

def load_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query, conn):
    output = pd.read_sql(query, conn)
    print(output)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("global-gdp-etl-pipeline/etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

log_progress("ETL Job Started")

log_progress("Extraction Started")
extracted_data = extract(url, table_attribs)
log_progress("Extraction Completed")

log_progress("Transformation Started")
transformed_data = transform(extracted_data)
log_progress("Transformation Completed")

log_progress("Loading data to csv started")
load_csv(transformed_data, target_file)
log_progress("Loading data to csv ended")

log_progress("Loading data to database started")
load_to_db(transformed_data, sql_conn, table_name)
log_progress("Loading data to database ended")

log_progress("Running Query on database")
run_query(f"SELECT * FROM {table_name} WHERE GDP_USD_billions > 100", sql_conn)
log_progress('Process Complete.')

sql_conn.close()
log_progress("ETL Job Finished")

