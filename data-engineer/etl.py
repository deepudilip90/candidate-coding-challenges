import mysql.connector
import json
from os import path, chdir, getcwd
from pprint import pprint
from tabulate import tabulate
import time

def read_json_data(filename, base_path=''):
    if not base_path:
        base_path = path.dirname(path.realpath(__file__))
    with open(path.join(base_path, filename), 'r') as f:
        data = json.load(f)
    return data

def _create_db_connection(host='mysqldb', port=3306, user='root', password='p@ssw0rd1', database=None):
    max_retries = 10
    tries = 0
    conn_args = {'host': host, 'port': port, 'user': user, 'password': password, 'database': database}
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
    

def init_db_and_create_tables():
    
    db_conn = _create_db_connection()
    
    cursor = db_conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS take_home")
    cursor.execute("CREATE DATABASE take_home")
    cursor.close()
    db_conn.close()

    db_conn = _create_db_connection(database='take_home')
    cursor = db_conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS certificates")
    cursor.execute("CREATE TABLE certificates (course VARCHAR(255), user VARCHAR(255), completedDate VARCHAR(255), startDate VARCHAR(255))")
    
    cursor.execute("DROP TABLE IF EXISTS courses")
    cursor.execute("CREATE TABLE courses (id VARCHAR(255), title VARCHAR(255), description VARCHAR(255), publishedAt VARCHAR(255))")
    
    cursor.execute("DROP TABLE IF EXISTS users")
    cursor.execute("CREATE TABLE users (id VARCHAR(255), email VARCHAR(255), firstName VARCHAR(255), lastName VARCHAR(255))")
    
    cursor.close()
    db_conn.close()

    print('database initialized!')

    return 

def load_data(data, table_name, fields, database='take_home', mode='replace', **kwargs):

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
            field_string = ', '.join([str(key) for key in record.keys() if key in fields])
            value_string = '", "'.join([value for key, value in record.items() if key in fields])
            insert_string = f'INSERT INTO {table_name} (' + field_string + ') VALUES ("' + value_string + '");'
            # print(insert_string)
            cursor.execute(insert_string)
            inserted_records += 1
        except Exception as err:
            print(f'Failed to insert record {record} due to error {err}')
    db_conn.commit()
    print(f'{inserted_records} out of a total of {len(data)} records inserted')
    
    cursor.close()
    db_conn.close()
    return True


def run_query_against_db(query_string, database='take_home'):
    db_conn = _create_db_connection(database=database)
    cursor = db_conn.cursor()

    cursor.execute(query_string)

    cursor.close()
    db_conn.close()

def perform_analysis(sql_file_name, output_columns, analysis_description='', database='take_home'):

    db_conn = _create_db_connection(database=database)
    cursor = db_conn.cursor()

    sql_folder_path = path.join(path.dirname(path.realpath(__file__)), 'sql_queries')
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

    db_conn.close()
    cursor.close()


if __name__ == '__main__':
    # set path to current directory
    # chdir(path.dirname(path.realpath(__file__)))

    print('Reading data from files...\n')
    courses = read_json_data('courses.json')
    certificates = read_json_data('certificates.json')
    users = read_json_data('users.json')
    
    print('Initializing database and creating tables...\n')
    init_db_and_create_tables()

    print('Loading data to database...\n')
    load_data(courses, 'courses', fields = ['id', 'title', 'description', 'publishedAt'], mode='append')
    load_data(certificates, 'certificates', fields = ['course', 'user', 'completedDate', 'startDate'], mode='append')
    load_data(users, 'users', fields = ['id', 'email', 'firstName', 'lastName'], mode='append')
    
    print('Running analytics queries against database...\n')
    perform_analysis(sql_file_name='fastest_users.sql', 
                     output_columns=['user_id', 'user_email', 'course_id', 'course_title', 'days_to_complete'],
                     analysis_description='fastest 5 users completing a course')
    
    perform_analysis(sql_file_name='slowest_users.sql',
                     output_columns = ['user_id', 'user_email', 'course_id', 'course_title', 'days_to_complete'],
                     analysis_description='slowest 5 users completing a course')
    
    perform_analysis(sql_file_name='avg_complete_time_per_course.sql',
                     output_columns=['course_id', 'course_title', 'average_days_to_complete'],
                     analysis_description='average amount of users time spent for each course individually')
    
    perform_analysis(sql_file_name='avg_complete_time_courses.sql',
                     output_columns=['average_days_to_complete'],
                     analysis_description='average days to complete a course (across all courses)')
    


