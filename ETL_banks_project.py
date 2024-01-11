# Code for ETL operations on Country-GDP data
"""Project Scenario
A multi-national firm has hired you as a data engineer. Your job is to access and process data as per requirements.
Your boss asked you to compile the list of the top 10 largest banks in the world ranked by market capitalization in
billion USD. Further, you need to transform the data and store it in USD, GBP, EUR, and INR per the exchange rate
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

# declared variable

URL = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
Exch_Rate = "exchange_rate.csv"
Table_Attributes = ['Name', 'MC_USD_Billion']
Final_Table_Attributes = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
Output_File = "Largest_banks_data.csv"
Database_name = "Bank.db"
Table_name = "Largest_banks"
Log_file = "code_log.txt"


def Log_progress(message):
    """ This function logs the mentioned message of a given stage of the
       code execution to a log file. Function returns nothing"""
    Time_Stamp_Format = "%Y-%h-%d, %H:%M:%S"
    nowTime = datetime.now()
    Time_Format = nowTime.strftime(Time_Stamp_Format)
    with open(Log_file, "a") as L:
        L.write(f"{Time_Format} :  {message} \n")

def Extract(URl,Table_attributes):
    """This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing."""
    Page = requests.get(URl).text
    content = BeautifulSoup(Page, "html.parser")
    data_frame = pd.DataFrame(columns=Table_attributes)
    tables = content.find_all("tbody")
    Rows = tables[0].find_all('tr')
    for row in Rows:
        Col = row.find_all('td')
        print(Col)


Extract(URL, Table_Attributes)



"""
# Declaring known values
Log_progress("Preliminaries complete. Initiating ETL process")
# Call extract() function
Log_progress("Data extraction complete. Initiating Transformation process")
# Call transform() function
Log_progress("Data transformation complete. Initiating Loading process")
# Call load_to_csv()
Log_progress("Data saved to CSV file")
# Initiate SQLite3 connection
Log_progress("SQL Connection initiated")
# Call load_to_db()
Log_progress("Data loaded to Database as a table, Executing queries")
# Call run_query()
Log_progress("Process Complete")
# Close SQLite3 connection
Log_progress("Server Connection closed")
"""
