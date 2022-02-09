# Import libraries
from google.cloud import bigquery
import pandas as pd

# Insert your project id 
project_id = "your-project-id"
# Add your stitch source datasets to the list
stitch_datasets = [
    'mysql_db',
    'mongo_db',
    'google_search_console'
]


# Loop through provided datasets
for dataset in stitch_datasets:
    # Get tables in dataset
    client = bigquery.Client(project_id)
    tables = client.list_tables(dataset)
    # Add table names to list
    tables_currently_in_destination_dataset = []
    for table in tables:
        tables_currently_in_destination_dataset.append(table.table_id)
    # loop through list of tables in dataset and get column names from each table
    for table in tables_currently_in_destination_dataset:
        # Get column names from the table in or to check for the column we want to partition by 
        query = """
        SELECT
          column_name
        FROM (
          SELECT
            *
          FROM
            {dataset_name}.INFORMATION_SCHEMA.COLUMNS )
        WHERE
          table_name = "{table_name}"
        """.format(dataset_name = dataset, table_name = table)
        client = bigquery.Client(project_id)
        table_column_names = client.query(query).to_dataframe()
        # If a table has a _sdc_batched_at column, we want to partition the table by it
        if table_column_names['column_name'].str.contains('_sdc_batched_at').any():
            # If upsert is used as the insert method Stitch will create a table called _sdc_primary_keys with a lookup of primary keys we want to group by
            if '_sdc_primary_keys' in tables_currently_in_destination_dataset:
                # Get the primary key(s) for the table we are currently looping through
                query = """
                SELECT
                  column_name
                FROM (
                  SELECT
                    *
                  FROM
                    `{project_id}.{dataset_name}._sdc_primary_keys` )
                WHERE
                  table_name = "{table_name}"
                """.format(project_id=project_id,dataset_name = dataset, table_name = table)
                primary_keys = client.query(query).to_dataframe()
                # Save it as a comma seperated string
                cluster_by_keys = ", ".join(primary_keys['column_name'].tolist())
                # If the table has any primary keys
                if len(cluster_by_keys) > 0:
                    # Create the table as a partitioned and clusered by table
                    query = """
                    create or replace table {dataset_name}.copy_{table_name}
                    partition by date( _sdc_batched_at )  
                    cluster by {cluster_by_keys} as select * from {dataset_name}.{table_name}; drop table {dataset_name}.{table_name};
                    create or replace table {dataset_name}.{table_name}
                    partition by date( _sdc_batched_at )  
                    cluster by {cluster_by_keys} as select * from {dataset_name}.copy_{table_name}; drop table {dataset_name}.copy_{table_name};
                    """.format(dataset_name = dataset, table_name = table, cluster_by_keys=cluster_by_keys)
                    response = client.query(query)
                    print('{dataset_name}.{table_name} has now been partitioned and clustered by {cluster_by_keys}'.format(dataset_name = dataset, table_name = table, cluster_by_keys=cluster_by_keys))
                else:
                    # If no primary keys were found for the table, then just partition it by _sdc_batched_at
                    query = """
                    create or replace table {dataset_name}.copy_{table_name}
                    partition by date( _sdc_batched_at )  as select * from {dataset_name}.{table_name}; drop table {dataset_name}.{table_name};
                    create or replace table {dataset_name}.{table_name}
                    partition by date( _sdc_batched_at ) as select * from {dataset_name}.copy_{table_name}; drop table {dataset_name}.copy_{table_name};
                    """.format(dataset_name = dataset, table_name = table)
                    response = client.query(query)
                    print('{dataset_name}.{table_name} has now been partitioned'.format(dataset_name = dataset, table_name = table))
            # In case append is used as insert method the primary table will not excist and we will just partition the table
            else:
                query = """
                create or replace table {dataset_name}.copy_{table_name}
                partition by date( _sdc_batched_at )  as select * from {dataset_name}.{table_name}; drop table {dataset_name}.{table_name};
                create or replace table {dataset_name}.{table_name}
                partition by date( _sdc_batched_at ) as select * from {dataset_name}.copy_{table_name}; drop table {dataset_name}.copy_{table_name};
                """.format(dataset_name = dataset, table_name = table)
                response = client.query(query)
                print('{dataset_name}.{table_name} has now been partitioned'.format(dataset_name = dataset, table_name = table))
        else:
            print('{dataset_name}.{table_name} does not contain _sdc_batched_at and is unchanged'.format(dataset_name = dataset, table_name = table))
print('All datasets has now been processed')
