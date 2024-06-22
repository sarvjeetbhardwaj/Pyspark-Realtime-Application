
## writing data into hive metastore
def data_hive_persist_cities(spark, df, dfname, partitionBy, mode):
    spark.sql('Create database if not exists cities')
    spark.sql('use cities')

    df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)

## writing data into hive metastore
def data_hive_persist_prescibers(spark, df, dfname, partitionBy, mode):
    spark.sql('Create database if not exists prescribers')
    spark.sql('use prescribers')

    df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)


