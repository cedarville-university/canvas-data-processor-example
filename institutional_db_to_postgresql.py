import logging
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text, URL, create_engine, exc, inspect

############################################################
# Logging Information
############################################################

load_dotenv()
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("Coll to PGSql")
logger.setLevel(logging.INFO)
formatter = logging.Formatter()
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

############################################################
# Connection to MSSQL ODS_Production
############################################################

# Login Connection Info
username = "username"
password = "password"
server = "institution_server"
db = "ODS_Colleague_Data"
driver = "ODBC+Driver+18+for+SQL+Server"
# trust = "TrustServerCertificate=yes"
# encrypt = "Encrypt=yes"


############################################################
# Creates the Connection to MSSQL
############################################################

# if encryption and trust is needed on your connection
# engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{db}?driver={driver}&{encrypt}&{trust}')
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{db}?driver={driver}')

############################################################
# Connection to PostGreSql ODS_Production
############################################################

# Login Connection Info
postgrehost = "postgres_server"
postgreport = "5432"
postgredatabase = "name_of_server"
postgreuser = "user"
postgrepassword = "password"

############################################################
# Creates the Connection to PGSql
############################################################

url_object = URL.create("postgresql+psycopg2", postgreuser, postgrepassword, postgrehost, postgreport, postgredatabase)
postgresql_engine = create_engine(url_object)


############################################################
# Declaring the Get_Query Function
############################################################

# Getting the Table names from ODS and creating the query's
def get_queries():
    init_query = "SELECT * FROM "
    return_list = []
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    for table_name in table_names:
        tablename = table_name
        query = init_query + tablename
        return_list.append((query, tablename))

    return return_list


############################################################
# Declaring the Get_Table Function
############################################################

def get_table(source, query):
    # Attempt to connect to the database by executing a simple query
    try:
        with source.connect() as connection:
            # Once connected to the ODS server we query the table and put it into a Dataframe
            df_query = connection.execute(text(query))
            df = pd.DataFrame(df_query.fetchall())
            return df
    except exc.OperationalError as e:
        print(f"ODS Connection failed: {e}")

    connection.close()


############################################################
# Declaring the Create_Table Function
############################################################

def create_table(dest, tablename, df):
    df.to_sql(con=dest, name=tablename, if_exists='replace')


############################################################
# Running the scripts
############################################################

queries = get_queries()
for data, table_name in queries:
    table = get_table(engine, data)
    create_table(postgresql_engine, table_name, table)
    print(table_name)

print("We finished...")
