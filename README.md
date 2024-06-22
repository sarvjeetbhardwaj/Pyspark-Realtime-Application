# Pyspark-Realtime-Application
In this project , we will create an ETL pipeline to extract data from Amazon S3 bucket, process and transform using Pyspark and load into Hive MetaStore

## Techstack used
 - Python3.9
 - AWS S3
 - AWS EMR
 - PySpark
 - AWS EC2
 - Hive

## Steps followed:
    1. Load the data into S3 bucket (This data can come from anysource, in this case we have loaded the  data manually into S3 bucket).The bucket structure will be similiar to the source folder.
    2. Create an EMR cluster with Pyspark, Hive & Hadoop installed.
    3. SSH into the primary node of EMR cluster a code editor (in this case , we used Visual Studio).
    5. Install required libraries --> pip install pyspark
    4. Create spark session.
        spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()
    4. Ingest the data
        - Copy the data from S3 bucket to source folder using the following command
          os.system(f'aws s3 cp s3://{bucket_name}/{bucket_folder} ./{destination} --recursive')
        - Load the data into respective dataframes via different file format method (in this case csv & parquet)
            df = spark.read.parquet(file_path)
            df = spark.read.csv(file_path, header=header, inferSchema=inferSchema)
    5. DataPreProcessing
        df_city_sel = df1.select(upper(df1.city).alias('City'),df1.state_id, 
                                upper(df1.state_name).alias('State_Name'), 
                                upper(df1.county_name).alias('Country_Name'),
                                df1.population, df1.zips)
        df_presc_sel = df2.select(df2.npi.alias('pres_id'),
                                  df2.nppes_provider_last_org_name.alias('presc_lname'),
                                  df2.nppes_provider_first_name.alias('presc_fname'),
                                  df2.nppes_provider_city.alias('presc_city'),
                                  df2.nppes_provider_state.alias('presc_state'),
                                  df2.specialty_description.alias('presc_specilaity'),
                                  df2.drug_name,df2.total_claim_count.alias('claim_count'),
                                  df2.total_day_supply,df2.total_drug_cost,
                                  df2.years_of_exp
                                  )
        df_presc_sel = df_presc_sel.withColumn('Country', lit('USA'))

        df_presc_sel = df_presc_sel.withColumn('years_of_exp', regexp_replace(df_presc_sel.years_of_exp,'=', ''))

        df_presc_sel = df_presc_sel.withColumn('years_of_exp', df_presc_sel.years_of_exp.cast('integer'))

        df_presc_sel = df_presc_sel.withColumn('presc_fullname', concat_ws(' ', df_presc_sel.presc_fname, df_presc_sel.presc_lname))

        df_presc_sel = df_presc_sel.drop(df_presc_sel.presc_lname, df_presc_sel.presc_fname)

        # checking for null values 
        df_presc_sel = df_presc_sel.select([count(when (isnan(c)|col(c).isNull(),c)).alias(c) for c in df_presc_sel.columns])

        #dropping null values based on certain columns
        df_presc_sel = df_presc_sel.dropna(subset='pres_id')
        df_presc_sel = df_presc_sel.dropna(subset='drug_name')

        # handling null values
        mean_claim_amount = df_presc_sel.select(mean(df_presc_sel.claim_count)).collect()[0][0]
        df_presc_sel = df_presc_sel.fillna({'claim_count': mean_claim_amount})

    6. Create a combined df frames using join operation
        df_presc_group = df_presc_sel.groupBy(df_presc_sel.presc_state, df_presc_sel.presc_city).agg(countDistinct('pres_id').alias('pres_count'),sum('claim_count').alias('claim_sum'))

        df_city_join = df_city_split.join(df_presc_group, (df_city_sel.state_id == df_presc_group.presc_state) & (df_city_sel.City == df_presc_group.presc_city ), 'inner') 

        df_final = df_city_join.select('City', 'State_Name', 'Country_Name',  'population', 'zipcount', 'pres_count')

    7. Filter out the top 5 states where claim count is highest and years of experience between 20 & 50
        spec = Window.partitionBy('presc_state').orderBy(col('claim_count').desc())
        df_pres_final = df_presc_sel.select('pres_id', 'presc_fullname', 'presc_state', 'claim_count', 'years_of_exp', 'total_day_supply').filter((col('years_of_exp')>=20) & (col('years_of_exp')<=50)).withColumn('dense_rank', dense_rank().over(spec)).filter(col('dense_rank') <=5)

    8. Write the data into hive metastore
        spark.sql('Create database if not exists cities')
        spark.sql('use cities')
        df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)
    