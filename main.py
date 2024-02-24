import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import sqlite3

Data_Url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
Table_attributes_upon_Extraction = ['Name', 'MC_USD_Billion']
Table_Attributes_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_csv_path = './Largest_banks_data.csv'
Database_name = 'Banks.db'
Table_name = 'Largest_banks'
log_file = 'code_log.txt'
Sql_connection = sqlite3.connect(Database_name)


def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attributes):
    df = pd.DataFrame(columns=table_attributes)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            title = cols[1].find('a')
            data_dict = {
                'Name': title.get('title'),
                'MC_USD_Billion': cols[2].text.strip()
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df):
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * 0.93)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * 82.95)
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * 0.8)

    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    return df


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='append', index=False)


def run_queries(query_statement, sql_connection):
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


try:
    log_progress("Starting data extraction...")
    result = extract(Data_Url, Table_attributes_upon_Extraction)
    log_progress("Data extraction completed.")

    log_progress("Starting data transformation...")
    transformed_df = transform(result)
    log_progress("Data transformation completed.")

    log_progress("Saving data to CSV...")
    load_to_csv(transformed_df, output_csv_path)
    log_progress("Data saved to CSV.")

    log_progress("Loading data to database...")
    load_to_db(transformed_df, Sql_connection, Table_name)
    log_progress("Data loaded to database.")

    log_progress("Running queries...")
    run_queries(f'SELECT * FROM {Table_name}', Sql_connection)
    run_queries(f'SELECT AVG(MC_GBP_Billion) FROM {Table_name}', Sql_connection)
    run_queries(f'SELECT Name from {Table_name} LIMIT 5', Sql_connection)
    log_progress("Queries executed successfully.")

finally:
    Sql_connection.close()  # Ensure the SQL connection is closed after use

