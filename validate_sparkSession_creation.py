from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_current_date(spark):
    try:
        output = spark.sql("""Select current_date""")
    except Exception as e:
        print(f'An error occured while validating spark object with get_current_date :: {e}')
    else:
        print('spark object Validated')

def print_schema(df,df_name):
    try:
        schema = df.printSchema()
        return f'Schema for dataframe {df_name} is {schema}'
    except Exception as e:
        print('An exception occured while getting the schema information {e}')

def check_for_nulls(df,df_name):
    try:
        df_null_check = df.select([count(when (isnan(c)|col(c).isNull(),c)).alias(c) for c in df.columns])
        df_null_show = df_null_check.show()
    except Exception as e:
        print(f'Failure in finding null values in each column {e}')
    else:
        print(f'Checking for null values in {df_name}')
        print(f'{df_null_show}')

