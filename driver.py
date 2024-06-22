import get_env_variables
from create_spark_session import create_spark_object
from validate_sparkSession_creation import get_current_date, print_schema, check_for_nulls
import os
import sys
from ingest import load_files,display_df,df_count,copy_read_file
from data_processing import data_clean
from data_transformation import *
from extraction import extract_files
from persist import *


def main():
    try:
        spark = create_spark_object(appName=get_env_variables.appName)
        get_current_date(spark=spark)

        print('Copying file from S3 bucket to local OLAP folder')
        file_format,header,inferSchema,file_path = copy_read_file(bucket_name=get_env_variables.bucket_name,
                       bucket_folder=get_env_variables.src_olap,
                       destination='./source/olap')

        df_city = load_files(file_format=file_format, 
                             spark=spark, 
                             file_path=file_path, 
                             header=header, 
                             inferSchema=inferSchema)
        print(f'Number of rows in city_df {df_count(df=df_city)}')

        print('Copying file from S3 to local OLTP folder')
        file_format,header,inferSchema,file_path = copy_read_file(bucket_name=get_env_variables.bucket_name,
                                                                  bucket_folder=get_env_variables.src_oltp,
                                                                  destination='./source/oltp')
        df_fact = load_files(file_format=file_format, 
                                   spark=spark, 
                                   file_path=file_path , 
                                   header=header, 
                                   inferSchema=inferSchema)
        print(f'Number of rows in medicare_df {df_count(df=df_fact)}')
        df_city_sel, df_presc_sel = data_clean(df1=df_city,
                                               df2=df_fact)

        display_df(df_city_sel)
        print_schema(df=df_city_sel, df_name='df_city_sel')
        
        display_df(df_presc_sel)
        print_schema(df=df_presc_sel, df_name='df_presc_sel')

        check_for_nulls(df=df_presc_sel, df_name='df_presc_sel')

        df_city = data_report(df_city_sel=df_city_sel, df_presc_sel=df_presc_sel)
        df_presc = data_report2(df_presc_sel = df_presc_sel)

        display_df(df_city)
        display_df(df_presc)

        city_path = get_env_variables.city_path
        extract_files(df=df_city, format='orc', filepath=city_path, splitno=1, headereq=False, compressiontype='snappy')

        presc_path = get_env_variables.presc_path
        extract_files(df=df_presc, format='parquet', filepath=presc_path, splitno=2, headereq=False, compressiontype='snappy')

        print('Data being written into hive ............... ')
        data_hive_persist_cities(spark=spark, df=df_city, dfname='df_city', partitionBy='state_name', mode='append')
        data_hive_persist_prescibers(spark=spark, df=df_presc, dfname='df_presc', partitionBy='presc_state', mode='append')
        print('Data writtten into hive metastore completed')
        
    except Exception as e:
        print(f'An exception occured in driver-main function --{e}')
    
if __name__ == '__main__':
    main()
    print('Application completed')