import os
import get_env_variables

def load_files(spark, file_path, header, inferSchema,file_format):

    try:
        if file_format == 'parquet':
            df = spark.read.parquet(file_path)
        elif file_format == 'csv':
            df = spark.read.csv(file_path, header=header, inferSchema=inferSchema)

    except Exception as e:
        print(f'Error while loading file in DataFrame -- {e}')
    else:
        return df

def display_df(df):
    df_show = df.show()
    return df_show

def df_count(df):
    df_count = df.count()
    return df_count

def copy_read_file(bucket_name,bucket_folder,destination):
    try:
        os.system(f'aws s3 cp s3://{bucket_name}/{bucket_folder} ./{destination} --recursive')
        for file_name in os.listdir(f'{destination}'):
            if file_name.endswith('.parquet'):
                file_format = 'parquet'
                header = False
                inferSchema = False
                file_path = f'{destination}/{file_name}'
            elif file_name.endswith('.csv'):
                file_format = 'csv'
                header = get_env_variables.header
                inferSchema = get_env_variables.inferSchema
                file_path = f'{destination}/{file_name}'
    except Exception as e:
        print(f'An exception occurred n ingest file --{e}' )
    else:
        return file_format,header,inferSchema,file_path

    
