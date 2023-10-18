#!/bin/python3

import duckdb
import os

base_path = os.path.abspath('./artifacts/glove-100_nb5_logs/metrics/')
con = duckdb.connect()



def getStat(file_name, column):
    
    file_path = os.path.join(base_path, file_name)

    con.execute(f"CREATE TABLE my_table AS SELECT * FROM read_csv_auto('{file_path}',  delim=',', header=true)")
 
    query = f'''
    SELECT 
        AVG({column}) as avg_value,
    FROM my_table;
    '''
    
    result = con.query(query).fetchall()

    con.execute(f"DROP TABLE my_table;")

    return result



recall = getStat('search_and_index__recall_100_op_search_and_index__select_ann_limit_100___stat_Average___workload_cql_vector2.csv','value')
print(f"Recall: {recall}")
rate = getStat('search_and_index__result_success_workload_cql_vector2.csv','m1_rate')
print(f"Train Rate: {recall}")
rate = getStat('rampup__result_success_workload_cql_vector2.csv','m1_rate')
print(f"Test Rate: {recall}")
