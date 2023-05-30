
import csv
import pandas as pd
from sqlalchemy import create_engine

from sqlalchemy import create_engine

usr="oum-prod-eu"
pwd="Dv45WDnqD6dV"
server="sql.eu.octopai-corp.local"
database="ucb-acc_Prod"

conn_string = f"mssql+pyodbc://{usr}:{pwd}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"




def ExtractETL(table_name,CONNECTION_ID,output_file):
    try:
        engine = create_engine(conn_string)
    except Exception as e:
        print(e)
    # Define the table name
    # SQL query to select all data from the table
    query = f'SELECT * FROM {table_name} where CONNECTION_ID={CONNECTION_ID}'
    # Read the data from the table into a pandas DataFrame
    # Define the output CSV file path

    offset = 0
    batch_size = 100000
    order_column = "rnk"
    with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        while True:
            # SQL query to select a batch of data with pagination
            query = f'SELECT * ,row_number() over (partition by connection_id order by connection_id) as rnk FROM {table_name} WHERE CONNECTION_ID=108 ORDER BY {order_column} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY'

            # Read the batch of records from the database
            try:
                df = pd.read_sql(query, engine)
            except Exception as e:
                print(e)

            # Check if there are no more records to process
            if df.empty:
                break

            # Write the batch of records to the CSV file
            if offset == 0:
                writer.writerow(df.columns)  # Write the column headers
            writer.writerows(df.values)  # Write the data rows
            print(f"Batch{offset}")

            # Increment the offset for the next batch
            offset += batch_size
    print(f"Data from table '{table_name}' has been exported to '{output_file}'.")


table_name='TI.MNG_OCTOPAI_LINEAGE_SOURCE_TO_TARGET'
CONNECTION_ID=108
output_file="E:\\tmp\\QlikSense\\QlikSenseETL.csv"

ExtractETL(table_name,CONNECTION_ID,output_file)



table_name='ti.MNG_LINEAGE_UI_QLIKSENSE_SOURCE_TO_TARGET'
CONNECTION_ID=108
output_file="E:\\tmp\\QlikSense\\QlikSenseReport.csv"

ExtractETL(table_name,CONNECTION_ID,output_file)


