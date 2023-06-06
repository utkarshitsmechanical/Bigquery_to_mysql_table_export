import json
import pymysql.cursors
import MongoDBConnection
constants = MongoDBConnection.getConstants()

# first put the user password and hostname
USER_= ""
PASSWORD_= ""
HOST_NAME= ""


def print_table_columns(Column_with_datatype,table_name):
    f = open("tables_schema.txt", "a")
    columns = []
    for i in Column_with_datatype:
        columns.append(i)
    f.write(str(len(columns)))
    f.write("schema_for_table----------> {table_name} ".format(table_name=table_name))
    start = "ArrayList < String > schemaList = new ArrayList < String > (Arrays.asList("
    columns = str(columns)
    index = slice(1, len(columns) - 1)
    start = start+str(columns[index]).replace("'","\"")+"))"
    f.write(start+";\n")
    f.close()

def print_table_dattypes(Column_with_datatype,table_name):
    f = open("tables_schema.txt", "a")
    column_datatype = []
    for i in Column_with_datatype:
        column_datatype.append(Column_with_datatype[i])
    f.write(str(len(column_datatype)))
    f.write("datatypes_for_table----------> {table_name} ".format(table_name=table_name))
    start = "ArrayList < String > datatypes = new ArrayList < String > (Arrays.asList("
    column_datatype = str(column_datatype)
    index = slice(1, len(column_datatype) - 1)
    start = start+str(column_datatype[index]).replace("'","\"")+"))"
    f.write(start+";\n")
    f.close()

def convert_bigquery_schema_to_mysql(column_name,datatype):
    if datatype == "INT64":
        return "BIGINT(20)"
    elif datatype == "STRING":
        return "varchar(200)"
    elif datatype == "TIMESTAMP" or datatype == "DATETIME":
        return "DATE"
    elif datatype == "FLOAT64":
        return "FLOAT"

def create_start_query(table_name):
    return "CREATE TABLE `{table_name}` (".format(table_name=table_name)

def is_primary_key(column_name,mysql_table_primary_key):
    if column_name in mysql_table_primary_key:
        return True
    else:
        return False

def create_end_query(Column_with_datatype,mysql_table_primary_key):
    siz = 0
    start_query = " ,PRIMARY KEY( "
    for i in Column_with_datatype:
        if is_primary_key(i,mysql_table_primary_key):
            start_query = start_query+"`{column_name}`,".format(column_name=i)
        siz = siz + 1

    start_query = start_query[0:-1:1]
    end_query = ")) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_unicode_ci"
    return start_query+end_query

def create_mid_query(Column_with_datatype,mysql_table_primary_key):
    size = len(Column_with_datatype)
    mid_query = ""
    for i in Column_with_datatype:
        size = size - 1
        if Column_with_datatype[i]=="varchar(100)":
            if is_primary_key(i,mysql_table_primary_key):
                mid_query = mid_query + "`{column_name}` varchar(100) COLLATE utf8_unicode_ci ".format(column_name=i)+" NOT NULL"
            else:
                mid_query = mid_query + "`{column_name}` varchar(100) COLLATE utf8_unicode_ci ".format(
                    column_name=i) + "DEFAULT NULL"
        else:
            if is_primary_key(i,mysql_table_primary_key):
                mid_query = mid_query + "`{column_name}` {data_type} NOT NULL".format(column_name=i,data_type=Column_with_datatype[i])
            else:
                mid_query = mid_query + "`{column_name}` {data_type} DEFAULT NULL".format(column_name=i, data_type=
                Column_with_datatype[i])

        if size != 0:
            mid_query = mid_query+","

    return mid_query

def create_sql_query(Column_with_datatype,table_name,mysql_table_primary_key):
    start_query = create_start_query(table_name)
    mid_query = create_mid_query(Column_with_datatype,mysql_table_primary_key)
    end_query = create_end_query(Column_with_datatype,mysql_table_primary_key)
    return start_query+mid_query+end_query

def create_delete_sql_query(table_name):
    query = """DROP TABLE `{table_name}`;""".format(table_name=table_name)
    return query

def create_truncate_sql_query(table_name):
    query = "truncate {table_name};".format(table_name=table_name)
    return query

def delete_mysql_table(table_name,mysql_database):
    try:
        query = create_delete_sql_query(table_name)
        db_conn_api = pymysql.connect(host=HOST_NAME,
                                      user=USER_, password=PASSWORD_,
                                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, database=mysql_database)
        db_conn_api.cursor().execute(query)
        print("table_delete_{table_name} --------------------------------->".format(table_name=table_name))
        return 1

    except Exception as e:
        print("error_encountered  ----------------------------------------->")
        print(e)
        return 0

def truncate_mysql_table(table_name,mysql_database):
    try:
        query = create_truncate_sql_query(table_name)
        db_conn_api = pymysql.connect(host=HOST_NAME,
                                      user=USER_, password=PASSWORD_,
                                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, database=mysql_database)
        db_conn_api.cursor().execute(query)
        print("table_truncate_{table_name} --------------------------------->".format(table_name=table_name))
        return 1

    except Exception as e:
        print("error_encountered  ----------------------------------------->")
        print(e)
        return 0


def create_mysql_table(Column_with_datatype,table_name,mysql_table_primary_key):
    try:
        query = create_sql_query(Column_with_datatype,table_name,mysql_table_primary_key)
        db_conn_api = pymysql.connect(host=HOST_NAME,
                                      user=USER_, password=PASSWORD_,
                                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, database='temp_data')
        db_conn_api.cursor().execute(query)
        print("table_created --------------------------------->")
        return 1

    except Exception as e:
        print("error_encountered  ----------------------------------------->")
        print(e)
        return 0
