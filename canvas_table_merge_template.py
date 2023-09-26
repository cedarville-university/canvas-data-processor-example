import os
from datetime import date
import logging
import pandas as pd
from sqlalchemy import URL, create_engine, text
from dotenv import load_dotenv
import time

#############################################################
# Connection information for the Postgresql for Canvas Data 2
#############################################################

load_dotenv()
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger("dap")
log.setLevel(logging.DEBUG)

pghost: str = os.environ["CANVAS_DATA_HOST"]
pgport: str = os.environ["CANVAS_DATA_PORT"]
pgdb: str = os.environ["CANVAS_DATA_DATABASE"]
pguser: str = os.environ["CANVAS_DATA_USER"]
pgpw: str = os.environ["CANVAS_DATA_PASSWORD"]

start = time.time()

#############################################################
# Initial Connection Functions and Checks
#############################################################


# Function creates the engine to connect to the pgsql server
def canvas_post_engine(host, port, db, user, pw):
    url_object = URL.create("postgresql+psycopg2", user, pw, host, port, db)
    engine = create_engine(url_object)
    return engine


# Function returns the current term based on today's date
def get_current_term():
    # Using today's date to decide the term
    current_day = date.today().strftime('%d')
    current_month = date.today().strftime('%m')
    current_year = date.today().strftime('%Y')
    current_daymonth = current_month + "/" + current_day
    term = ""

    # Here we set the generic start date of the fall term
    fall_day = "01"
    fall_month = "08"
    fall_date = fall_month + "/" + fall_day

    # Here we set the generic start date of the spring term
    spring_day = "01"
    spring_month = "01"
    spring_date = spring_month + "/" + spring_day

    # Here we set the generic start date of the summer term
    summer_day = "01"
    summer_month = "05"
    summer_date = summer_month + "/" + summer_day

    # We decide what term it is and return it
    if current_daymonth > fall_date:
        term = "Fall Semester " + current_year
    elif current_daymonth > spring_date:
        if current_daymonth < summer_date:
            term = "Spring Semester " + str(int(current_year) + 1)
        else:
            term = "Summer Semester " + str(int(current_year) + 1)

    return term


# Function checks and returns boolean if data is located in the current table
def check_term_data_in_db(engine, term):
    # Connecting to the ODS Table in PGSql to retrieve Data
    with engine.connect() as connect:
        # This is the Enrollment block, selecting, executing, renaming columns #
        sql = '''SELECT * FROM public.pg_table_name WHERE public.pg_table_name.enrollment_term_name
            = :a FETCH FIRST 1 ROWS ONLY'''
        sql_text = text(sql)
        sql_text = sql_text.bindparams(a=term)
        query = connect.execute(sql_text)
        df = pd.DataFrame(query.fetchall())

    if not df.empty:
        return True
    else:
        return False


# Function checks and returns boolean if data is located in the current table
def get_current_term_data_in_db(engine):
    # Connecting to the ODS Table in PGSql to retrieve Data
    with engine.connect() as connect:
        # This is the Enrollment block, selecting, executing, renaming columns #
        sql = '''SELECT * FROM public.pg_table_name'''
        sql_text = text(sql)
        query = connect.execute(sql_text)
        df = pd.DataFrame(query.fetchall())

    return df


#############################################################
# Data Frame Functions
#############################################################

# Function selects the sql query and returns df from accounts
def select_accounts(connection):
    # This is the Account block, selecting, executing, renaming columns #
    account_sql = '''SELECT (all_table_columns) FROM canvas.accounts'''
    account_query = connection.execute(text(account_sql))
    df = pd.DataFrame(account_query.fetchall())
    df = df.rename(columns={'id': 'account_id'})
    return df


# Function selects the sql query and returns df from courses
def select_courses(connection):
    # This is the Course block, selecting, executing, renaming columns #
    course_sql = '''SELECT (all_table_columns) FROM canvas.courses'''
    course_query = connection.execute(text(course_sql))
    df = pd.DataFrame(course_query.fetchall())
    df = df.rename(columns={'id': 'course_id'})
    return df


# Function selects the sql query and returns df from enrollments
def select_enrollments(connection, enrollment):
    # This is the Enrollment block, selecting, executing, renaming columns #
    enrollment_sql = '''SELECT (all_table_columns) FROM canvas.enrollments WHERE canvas.enrollments.type = :a'''
    enrollment_sql_text = text(enrollment_sql)
    enrollment_sql_text = enrollment_sql_text.bindparams(a=enrollment)
    enrollment_query = connection.execute(enrollment_sql_text)
    df = pd.DataFrame(enrollment_query.fetchall())
    df = df.rename(columns={'id': 'enrollment_id'})
    return df

# just copy function template above fore more tables...

# Function uses the dfs and merges all the tables together
def merge_tables_account(df, df2, names, value, jointype):
    merge_df = pd.merge(df, df2, left_on=value, right_on=value, how=jointype)
    # print_memory()
    return merge_df

###############################################################################
# These functions below are used for more memory and time duration testing
###############################################################################

# def print_memory():
#     log.info('')
#     log.info('Virtual RAM 1 Used (GB): %s', psutil.virtual_memory()[1]/1000000000)
#     log.info('Virtual RAM 3 Used (GB): %s', psutil.virtual_memory()[3]/1000000000)
#     log.info('Processed RAM Used (GB): %s', psutil.Process().memory_info().rss/1000000000)
#     log.info('')


def print_time(start_time, name):
    table_end = time.time()
    log.info('')
    log.info('%s duration in: %s sec', name, (table_end - start_time))
    log.info('%s duration in: %s min', name, ((table_end - start_time)/60))
    log.info('%s duration in: %s hour', name, ((table_end - start_time)/3600))
    log.info('')


def main():
    #############################################################
    # Variables
    #############################################################

    postgresql_engine = canvas_post_engine(pghost, pgport, pgdb, pguser, pgpw)
    current_term = get_current_term()
    current_data = check_term_data_in_db(postgresql_engine, current_term)
    current_table_name = "cd2_current_term"
    archived_table_name = "cd2_archive_term"
    enrollment_type = "StudentEnrollment"

    # Creating the df for each table
    # print_time(start, "initializing")
    with postgresql_engine.connect() as conn:
        df_accounts = select_accounts(conn)
        df_courses = select_courses(conn)
        df_enrollment = select_enrollments(conn, enrollment_type)
        # just copy above fore more data frames...

        merged_df = merge_tables_account(df_accounts, df_courses, "courses", "account_id", "inner")
        merged_df = merge_tables_account(merged_df, df_enrollment, "enrollment", "course_id", "left")
        # just copy above fore more merges...

        # If the current data is true then we will just replace the table with current term data
        # If the current data is false then we will just append the archived table with current term data and replace
        # the current table with current term data with a chunksize of 5000 to save on resources

        if not current_data:
            # It is not current data
            # Appending to the historical archive table of Canvas Data
            current_term_data_df = get_current_term_data_in_db(postgresql_engine)
            current_term_data_df.to_sql(archived_table_name, con=postgresql_engine, if_exists='append', chunksize=5000, method=None)

        # This is current data
        # We are replacing the current table of Canvas Data with new refresh
        # print_memory()
        # print_time(start, "about to push to server")
        merged_df.to_sql(current_table_name, con=postgresql_engine, if_exists='replace', chunksize=5000, method=None)
        # print_time(start, "finished pushing to server")
        # print_memory()

    # this just prints overall elapsed time so we can see how long it took to run
    print_time(start, "overall elapsed time")


if __name__ == "__main__":
    main()
