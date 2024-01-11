# Code for ETL operations on Country-GDP data
"""Project Scenario
A multinational firm has hired you as a data engineer. Your job is to access and process data as per requirements.
Your boss asked you to compile the list of the top 10 largest banks in the world ranked by market capitalization in
a billion USD. Further, you need to transform the data and store it in USD, GBP, EUR, and INR per the exchange rate
information made available to you as a CSV file. You should save the processed information table locally in a CSV
format and as a database table. Managers from different countries will query the database table to extract the list
and note the market capitalization value in their own currency."""

# import Libraries

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime


def Log_progress(message):
    """ This function logs the mentioned message of a given stage of the
       code execution to a log file. Function returns nothing"""
    Time_Stamp_Format = "%Y-%h-%d, %H:%M:%S"
    nowTime = datetime.now()
    Time_Format = nowTime.strftime(Time_Stamp_Format)
    with open(Log_file, "a") as L:
        L.write(f"{Time_Format} :  {message} \n")


def Extract(URl, Table_attributes):
    """This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing."""
    Page = requests.get(URl).text
    content = BeautifulSoup(Page, "html.parser")
    data_frame = pd.DataFrame(columns=Table_attributes)
    tables = content.find_all("tbody")
    Rows = tables[0].find_all("tr")
    for row in Rows:
        Col = row.find_all("td")
        if len(Col) >= 3:
            a_tags = Col[1].find_all("a")
            if len(a_tags) > 1:
                usd = float(Col[2].text.strip())
                data_dict = {"Name": a_tags[1].text, "MC_USD_Billion": usd}
                df = pd.DataFrame(data_dict, index=[0])
                data_frame = pd.concat([data_frame, df], ignore_index=True)

    return data_frame


def Transform(df, csv_path):
    """ This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies"""
    csv_df = pd.read_csv(csv_path)
    New_value = {"MC_EUR_Billion": [np.round(csv_df["Rate"].tolist()[0] * a, 2) for a in df["MC_USD_Billion"].tolist()],
                 "MC_GBP_Billion": [np.round(csv_df["Rate"].tolist()[1] * a, 2) for a in df["MC_USD_Billion"].tolist()],
                 "MC_INR_Billion": [np.round(csv_df["Rate"].tolist()[2] * a, 2) for a in df["MC_USD_Billion"].tolist()]}
    value_df = pd.DataFrame(New_value)
    df = pd.concat([df, value_df], axis=1, join='inner')
    return df


def load_to_csv(df, output_path):
    """ This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""
    return df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    """ This function saves the final data frame to a database
    table with the provided name. Function returns nothing."""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_s, sql_connection):
    """ This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. """
    Query = pd.read_sql(query_s, sql_connection)
    print(Query)


""" Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function"""

# Declaring known values
URL = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
Exch_Rate = "exchange_rate.csv"
Table_Attributes = ['Name', 'MC_USD_Billion']
Final_Table_Attributes = ['Name', 'MC_USD_Billion', 'MC_EUR_Billion', 'MC_GBP_Billion', 'MC_INR_Billion']
Output_File = "Largest_banks_data.csv"
Database_name = "Bank.db"
Table_name = "Largest_banks"
Log_file = "code_log.txt"
pd.set_option('display.max_columns', None)

Log_progress("Preliminaries complete. Initiating ETL process")
# Call extract() function
DF = Extract(URL, Table_Attributes)
Log_progress("Data extraction complete. Initiating Transformation process")
# Call transform() function
TDF = Transform(DF, Exch_Rate)
Log_progress("Data transformation complete. Initiating Loading process")
# Call load_to_csv()
load_to_csv(TDF, Output_File)
Log_progress("Data saved to CSV file")
# Initiate SQLite3 connection
SqlConnection = sqlite3.connect(Database_name)
Log_progress("SQL Connection initiated")
# Call load_to_db()
load_to_db(TDF, SqlConnection, Table_name)
Log_progress("Data loaded to Database as a table, Executing queries")
# Call run_query()
query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement, SqlConnection)
query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, SqlConnection)
query_statement = "SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, SqlConnection)
Log_progress("Process Complete")
# Close SQLite3 connection
SqlConnection.close()
Log_progress("Server Connection closed")

# Convert DataFrame to a string and print
# print(Transform(DF, Exch_Rate).to_string(index=False))
