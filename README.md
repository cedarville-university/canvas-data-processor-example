# canvas-data-processor-example

## DESCRIPTION

This "README" file provides an explanation of the Python scripts institutional_db_to_postgresql.py and canvas_table_merge_template.py

-------------------------------

## FILE OVERVIEW

- File Name: institutional_db_to_postgresql
- Author: Byamba Bird
- Date: September 30, 2023
- Version: 1.0 (Initial Release)
- 

## PURPOSE

The purpose of institutional_db_to_postgresql.py is to demonstrate and provide a generic template. This template is how we initially created the institutional data tables to copies hosted on our PGSql Server. The purpose of this data set is to allow the combination and joining of data in the same server for an easier access.


## PREREQUISITES

- Before using institutional_db_to_postgresql.py, make sure you have the following prerequisites:
- Python: Ensure that you have Python 3.9 installed on your system.
- Psycopg2: This is to allow you to connect to the PGSql.
- Pyodbc: To connect to a ODBC server (which our institution uses for Colleague)
- Sql-driver: ODBC Driver 18 for SQL Server

  
## USAGE

To use institutional_db_to_postgresql.py, follow these steps:
- Import the script into your Python project or environment
- Can execute the file or use it in a cron job once its set up correctly


## FUNCTIONALITY

The script provides the following functions:

get_queries(): return tables from your ODBC connection
create_table(a,b): creates table in your PGSql server using tables name (b) from institutional ODBC server (a)

-------------------------------

## FILE OVERVIEW

- File Name: canvas_table_merge_template.py
- Author: Byamba Bird
- Date: September 30, 2023
- Version: 1.0 (Initial Release)


## PURPOSE

The purpose of canvas_table_merge_template.py is to demonstrate and provide a generic template. This template is how we initially create a singular table or data set of Canvas Data 2. We used python pandas to join multiple tables that hosted on our PGSql Server. The purpose of this data set is to use it in Tableau or other reporting tools.


## PREREQUISITES

- Before using canvas_table_merge_template.py, make sure you have the following prerequisites:
- Python: Ensure that you have Python 3.9 installed on your system.
- Psycopg2: This is to allow you to connect to the PGSql.

  
## USAGE

To use canvas_table_merge_template.py, follow these steps:
- Import the script into your Python project or environment
- Can execute the file or use it in a cron job once its set up correctly


## FUNCTIONALITY

The script provides the following functions:

canvas_post_engine(a, b, c, d, e): Creates and connects postgresql using "a" "b" "c" "d" "e" and return the engine.
get_current_term(): Returns the current term in form as "Semester YEAR".
check_term_data_in_db(a, b): Checks the PGSQl database "a" using term "b" and returns a true if found else returns false.
get_current_term_data_in_db(a): returns a dataframe of the current term data from the PGSql.
select_table(a): using SQL query it selects and resturns the dataframe of the table using connection "a".
select_table(a, b): using SQL query it selects, filters on "b" and resturns the dataframe of the table using connection "a".
merge_tables_account(a, b, c, d): Returns a merged dataframe using tables "a" and "b" on the columns "c" with join type of "d".

-------------------------------

## LICENSE

This Python script is provided by Cedarville. You are free to use, modify, and distribute it in accordance with the terms of the license with the understanding that this is a template.

-------------------------------

## CONTACT INFORMATION

If you have any questions or need further assistance, feel free to contact the author:
- Name: Byamba Bird
- Email: bbbird@cedarville.edu

Thank you for using canvas_table_merge_template.py. We hope it helps you create and understand the collecting and merging of data to update a table in the PGSql.
