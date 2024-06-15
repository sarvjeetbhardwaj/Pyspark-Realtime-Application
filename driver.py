import get_env_variables
from create_spark_session import create_spark_object
from validate_sparkSession_creation import get_current_date
import os
import sys
from ingest import load_files

def main():
    try:
        spark = create_spark_object(appName=get_env_variables.appName)
        get_current_date(spark=spark)
    except Exception as e:
        print(f'An error occurred in the main method of the driver program :: {e}')

    print('Copying file from S3 to local OLAP folder')
    os.system(f'aws s3 cp s3://{get_env_variables.bucket_name}/{get_env_variables.src_olap} ./source/olap --recursive')

    for file_name in os.listdir(f'./source/{get_env_variables.src_olap}'):
        if file_name.endswith('.parquet'):
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'
        elif file_name.endswith('.csv'):
            file_format = 'csv'
            header = get_env_variables.header
            inferSchema = get_env_variables.inferSchema

    print(f'File on olap folder is of {file_format} format')

    



    

if __name__ == '__main__':
    main()
    print('Application completed')