# Web Scraping Project in Python - Certified by IBM

## Project Overview

This Python project focuses on performing ETL (Extract, Transform, Load) operations on data related to the top 10 largest banks in the world, ranked by market capitalization. The data is scraped from a Wikipedia page, transformed, and stored in different currencies (USD, GBP, EUR, and INR) based on exchange rate information provided in a CSV file. The processed data is saved both as a CSV file and in a SQLite database table.

## Project Structure

The project is organized into several functions, each responsible for a specific stage of the ETL process:

- `Extract`: Scrapes the required information from a Wikipedia page and creates a DataFrame.
- `Transform`: Adds columns to the DataFrame, transforming market capitalization to different currencies.
- `load_to_csv`: Saves the final DataFrame as a CSV file.
- `load_to_db`: Saves the final DataFrame to a SQLite database table.
- `run_query`: Executes queries on the SQLite database table.

## Usage

To run the project, follow these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/SOHAMRANA77/WebScraping1o1.git
2. Install the required dependencies:
   ```bash
    pip install requests beautifulsoup4 pandas numpy
   ```
3. Run the main script:
    ```bash
   python main_script.py
### Known Values
Wikipedia URL: List of largest banks\
Exchange Rate CSV: exchange_rate.csv\
Table Attributes: ['Name', 'MC_USD_Billion']\
Final Table Attributes: ['Name', 'MC_USD_Billion', 'MC_EUR_Billion', 'MC_GBP_Billion', 'MC_INR_Billion']\
Output CSV File: Largest_banks_data.csv\
Database Name: Bank.db\
Database Table Name: Largest_banks\
Log File: code_log.txt\
### Additional Notes
The project includes logging functionality to keep track of the execution stages.
SQLite database is used for storage and retrieval of data.
Feel free to explore the code and adapt it to your needs!

# Certification
This project is certified by IBM.
### id : LJAR89Z4LN43

## Strengths

1. **Clear Project Scenario:**
- The project scenario is well-defined, providing a clear understanding of the tasks the data engineer needs to accomplish. This makes the project purpose and goals transparent.

2. **Structured Code:**
- The code is well-organized with clear functions for different stages of the ETL process. This modular approach enhances code readability and maintainability.

3. **Documentation:**
- The use of docstrings for each function helps in understanding the purpose and functionality of the code. This documentation aids collaboration and future modifications.

4. **Use of Libraries:**
- The project utilizes popular Python libraries such as BeautifulSoup, Pandas, and SQLite3, demonstrating good practices in leveraging existing tools for efficient development.

5. **Logging:**
- The inclusion of a logging function (`Log_progress`) is a good practice for tracking the execution of different stages in the code. This can be valuable for debugging and monitoring.

6. **Error Handling:**
- The code includes conditional checks, ensuring that certain conditions are met before proceeding with specific operations. This helps prevent potential errors.

7. **SQL Queries:**
- SQL queries are well-structured and provide useful insights into the processed data. The queries demonstrate a good understanding of SQL and are relevant to the project's goals.

## Suggestions for Improvement

1. **Exception Handling:**
- Consider implementing more comprehensive exception handling within functions to catch potential errors and provide meaningful error messages. This can enhance the robustness of the code.

2. **Parameterization:**
- Parameterize constants such as the URL, exchange rate file name, output file name, database name, and table name. This allows for greater flexibility and easier adaptation to different scenarios.

3. **Consistent Naming Conventions:**
- Ensure consistent naming conventions throughout the code. For example, the function `Extract` uses uppercase attributes, while other functions use lowercase. Consistency improves code readability.

4. **Use of Global Variables:**
- Limit the use of global variables. While it may be necessary in certain situations, it's generally better to pass required parameters explicitly to functions to improve code maintainability.

5. **Comments:**
- While the docstrings provide good documentation for functions, consider adding inline comments within the code to explain complex or critical sections, making it even more understandable for readers.

6. **Testing:**
- Consider implementing unit tests for the functions to ensure their correctness and facilitate future changes without introducing regressions.
