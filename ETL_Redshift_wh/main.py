from redshift_read_load import read_local_csv, generate_schema, execute_sql, load_to_redshift, \
get_redshift_connection
from datetime import datetime


conn = get_redshift_connection()
def main():
    table_name = 'log_data'
    data = read_local_csv('data/log_data1.csv')
    data['date'] = data['date'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())
    schema_query = generate_schema(data, table_name)
    execute_sql(schema_query, conn)
    print('Schema generated in Redshift')
    load_to_redshift(table_name)



main()