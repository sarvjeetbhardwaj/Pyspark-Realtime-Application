from pyspark.sql import SparkSession


def create_spark_object(appName):
    try:
        spark = SparkSession.builder.appName(appName).getOrCreate()
    except Exception as e:
        print(f'An error occured while creating the spark object :: {e}')
    else:
        print(f'Spark object created')
        return spark
