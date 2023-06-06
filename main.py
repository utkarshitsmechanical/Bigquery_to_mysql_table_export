# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import json
import Configs
from google.cloud import bigquery
client = bigquery.Client.from_service_account_json('stellar-display-145814-c94dbb54049a.json')
# Press the green button in the gutter to run the script.


def get_mysql_datatype(schema_list):
    for i in schema_list:
        schema_list[i] = Configs.convert_bigquery_schema_to_mysql(i,schema_list[i])
    return schema_list

def remove_extra_space(schema_list_with_extra_space):
    schema_list_final = {}
    for i in range(0,len(schema_list_with_extra_space)):
        if i == len(schema_list_with_extra_space)-1:
            column = str(schema_list_with_extra_space[i])[3:]
            column = column[::-1]
            column = column[1:]
            column = column[::-1]
            column = column.split()
            schema_list_final[column[0]]=column[1]
        else:
            column = str(schema_list_with_extra_space[i])[3:]
            column = column.split()
            schema_list_final[column[0]] = column[1]
    return schema_list_final

if __name__ == '__main__':
    try:
        # Instantiate a BigQuery client object
        bigquery_table_name = {"mysql_database_name":"dataset_name.table_name"}
        mysql_table_primary_key = ["column_name1","column_name2"]

        # Entries to delete table from mysql
        delete_mysql_table_entries = ["databse_name.table_name"]

        # Entries to truncate table from mysql
        truncate_mysql_table = ["databse_name.table_name"]

        # Define your SQL query as a string
        print(len(bigquery_table_name))
        for table_name_full in bigquery_table_name:
            table_name_array = table_name_full.split(".")
            table_name = table_name_array[1]
            dataset_name = table_name_array[0]

            sql_query = """
                SELECT ddl FROM `{dataset_name}`.INFORMATION_SCHEMA.TABLES where table_name = "{table_name}";
            """.format(dataset_name=dataset_name,table_name=table_name)

            # Execute the SQL query and store the results in a 1iable
            query_job = client.query(sql_query)
            results = query_job.result()

            print("Working for table name ------------------> table_name "+table_name)

            # Print the results
            for row in results:
                datatypes = str(row[0])
                schema_start = str(datatypes).find("(")
                schema_end = str(datatypes).find(")")
                schema_final = datatypes[schema_start+1:schema_end]
                schema_list_with_extra_space = schema_final.split(",")
                schema_list = remove_extra_space(schema_list_with_extra_space)
                prepare_mysql_datatype = get_mysql_datatype(schema_list)

                Configs.print_table_columns(prepare_mysql_datatype,table_name)
                Configs.print_table_dattypes(prepare_mysql_datatype,table_name)
                Configs.create_mysql_table(prepare_mysql_datatype,table_name,mysql_table_primary_key)

        for mysql_table in delete_mysql_table_entries:
            table_name_array = mysql_table.split(".")
            table_name = table_name_array[1]
            dataset_name = table_name_array[0]
            print(" DELETING TABLE "+table_name)
            Configs.delete_mysql_table(mysql_table)
            print(" TABLE DELETED" + table_name + "------------------->")

        for mysql_table in truncate_mysql_table:
            table_name_array = mysql_table.split(".")
            table_name = table_name_array[1]
            dataset_name = table_name_array[0]
            print(" TRUNCATE TABLE " + table_name)
            Configs.truncate_mysql_table(mysql_table)
            print(" TABLE TRUNCATED" + table_name + "------------------->")

    except Exception as e:
        print("error encountered")
        print(e)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
