import mysql.connector
import json
from os import path
from tabulate import tabulate
import time
import csv

def read_json_data(filename, base_path=''):
    """
    Convenience method to read data from a json file.

    :param filename: The name of the file from which to read the data.
    :param basepath: The base path to the file.
    """
    if not base_path:
        base_path = path.dirname(path.realpath(__file__))
    with open(path.join(base_path, filename), 'r') as f:
        data = json.load(f)
    return data

def _create_db_connection(host='mysqldb', 
                          port=3306, 
                          user='root', 
                          password='p@ssw0rd1',
                          database=None):
    """
    Convenience method to be set up a connection to the database.

    :param host: The host address of the database server. When running 
                 via docker, this is specified in the docker-compose file.
    :param port: The port in which the database server accepts connections.
    :param user: The user name to connect to the database.
    :param password: The password to use to connect to the server.
    :param database: The database to which to connect to in the server.
    """
    max_retries = 10
    tries = 0
    conn_args = {'host': host,
                 'port': port,
                 'user': user,
                 'password': password,
                 'database': database}
    conn_args = {k: v for k, v in conn_args.items() if v}

    while tries <= max_retries:
        try:
            db_conn = mysql.connector.connect(**conn_args)
        except:
            print('Cannot connect to database. Retrying...')
            tries += 1
            db_conn = None
            time.sleep(3)
            continue
        if db_conn:
            print('Successfully connected to database!')
            break
    
  
    return db_conn
    

def _write_output_to_csv(data, output_path, headers):
    """
    Convenience method to write a list of tuples to a csv file.

    :param data: A list of tuples to be written as csv file.
    :param output_path: The path to which the csv file should be written.
    :param headers: The header fields for the csv file.
    """
    with open(output_path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(headers)
        for row in data:
            csv_writer.writerow(row)


def init_db_and_create_tables():
    """
    Function to set up a database in the database server and create the
    necessary tables.
    """
    
    db_conn = _create_db_connection()
    
    cursor = db_conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS take_home")
    cursor.execute("CREATE DATABASE take_home")
    cursor.close()
    db_conn.close()

    db_conn = _create_db_connection(database='take_home')
    cursor = db_conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS certificates")
    cursor.execute("""CREATE TABLE certificates
                     (course VARCHAR(255), user VARCHAR(255), 
                      completedDate VARCHAR(255), startDate VARCHAR(255))""")
    
    cursor.execute("DROP TABLE IF EXISTS courses")
    cursor.execute("""CREATE TABLE courses 
                    (id VARCHAR(255), title VARCHAR(255), 
                    description VARCHAR(255), publishedAt VARCHAR(255))""")
    
    cursor.execute("DROP TABLE IF EXISTS users")
    cursor.execute("""CREATE TABLE users (id VARCHAR(255), email VARCHAR(255), 
                      firstName VARCHAR(255), lastName VARCHAR(255))""")
    
    cursor.close()
    db_conn.close()

    print('database initialized!')

    return 

def load_data(data, 
              table_name, 
              fields, 
              database='take_home', 
              mode='replace'):
    """
    Function to load data from a list of records (json style) into a specific
    table in the database.

    :param data: The data to be loaded to the database. The data is in the form
                 of list of dictionaries, with each item being one row-level
                 record.
    :param table_name: The name of the table to which to load the data.
    :param fields: The columns in the table to which the data should be loaded.
    :param database: The name of the database.
    :param mode: If 'replace' all existing records will be deleted and new 
                 records inserted. Otherwise, the records will be appended.
    """

    db_conn = _create_db_connection(database=database)

    cursor = db_conn.cursor()

    inserted_records = 0
    if mode == 'replace':
        cursor.execute(f'DELETE FROM {table_name} WHERE 1=1')
        print('deleted any existing records!')
    elif mode == 'append':
        pass
    else:
        print('Wrong mode specified!!')
        return False

    print(f'Inserting records into table {table_name}')
    for record in data:
        try:
            field_string = ', '.join([str(key) for key in record.keys()
                                      if key in fields])
            value_string = '", "'.join([value for key, value in record.items()
                                        if key in fields])
            insert_string = (f'INSERT INTO {table_name} (' + 
                               field_string + ') VALUES ("' + 
                               value_string + '");')
            # print(insert_string)
            cursor.execute(insert_string)
            inserted_records += 1
        except Exception as err:
            print(f'Failed to insert record {record} due to error {err}')
    db_conn.commit()
    print(f'{inserted_records} out of a total of {len(data)} records inserted')

    if inserted_records != len(data):
        print('Not all records could be loaded!!!')
        return False
    
    cursor.close()
    db_conn.close()
    return True

def perform_analysis(sql_file_name, 
                     output_columns,
                     analysis_description='',
                     database='take_home',
                     sql_folder_path=None):
    """
    This function reads an sql query stored in a file and executes it against
    the database. The results are printed to console and also written to a csv
    file in the project working directory.

    :param sql_file_name: The name of the SQL file containing the query to be
                           executed.
    :param output_columns: The names of the columns expected in the output 
                            of the SQL query.
    :param analysis_description: A description to be printed to console when
                                 printing the results of the query.
    :param database: The name of the database against which the query should be
                     run.
    :param sql_folder_path: The path to the folder in which the SQL files are 
                            stored. If not specified a default path is used. 
    """

    db_conn = _create_db_connection(database=database)
    cursor = db_conn.cursor()

    if not sql_folder_path:
        sql_folder_path = path.join(path.dirname(path.realpath(__file__)),
                                    'sql_queries')
    with open(path.join(sql_folder_path, sql_file_name), 'r') as f:
        query = f.read()

    cursor.execute(query)

    result = cursor.fetchall() 

    # result_df = pd.DataFrame(result, columns=output_columns)
    if analysis_description:
        print('Results for ' + analysis_description)
    else:
        print('Result for query in ' + sql_file_name)

    print(tabulate(result, headers=output_columns))
    
    print('\n')
    output_base_path = path.dirname(path.realpath(__file__))
    output_file_path = (output_base_path + '/result_' + 
                        sql_file_name.replace('.sql', '') + '.csv')
   
    _write_output_to_csv(result, output_file_path, output_columns)

    db_conn.close()
    cursor.close()


def main():
    """
    Main function performing all the steps such as reading from json file, 
    loading to database, running the SQL queries and writing the output.
    """
    print('Reading data from files...\n')
    courses = read_json_data('courses.json')
    certificates = read_json_data('certificates.json')
    users = read_json_data('users.json')
    
    print('Initializing database and creating tables...\n')
    init_db_and_create_tables()

    print('Loading data to database...\n')
    if not load_data(courses, 
                     'courses', 
                     fields = ['id', 'title', 'description', 'publishedAt'], 
                     mode='append'):
        print('Warning: Failed to load all data for courses.json!')
    if not load_data(certificates, 
                     'certificates', 
                     fields = ['course', 'user', 'completedDate', 'startDate'],
                     mode='append'):
        print('Warning: Failed to load all data for certificates.json!')       
    
    if not load_data(users, 
              'users',
              fields = ['id', 'email', 'firstName', 'lastName'],
              mode='append'):
        print('Warning: Failed to load all data for users.json!')    
    
    print('Running analytics queries against database...\n')
    perform_analysis(sql_file_name='fastest_users.sql', 
                     output_columns=['user_id', 'user_email', 'course_id', 
                                     'course_title', 'days_to_complete'],
                     analysis_description='fastest 5 users completing a course')
    
    perform_analysis(sql_file_name='slowest_users.sql',
                     output_columns = ['user_id', 'user_email', 'course_id', 
                                       'course_title', 'days_to_complete'],
                     analysis_description='slowest 5 users completing a course')
    
    perform_analysis(sql_file_name='avg_complete_time_per_course.sql',
                     output_columns=['course_id', 'course_title',
                                     'average_days_to_complete'],
                     analysis_description="""average amount of users time
                                          spent for each course individually""")
    
    perform_analysis(sql_file_name='avg_complete_time_courses.sql',
                     output_columns=['average_days_to_complete'],
                     analysis_description="""average days to complete a course
                                             (across all courses)""")
    

if __name__ == '__main__':
    main()

