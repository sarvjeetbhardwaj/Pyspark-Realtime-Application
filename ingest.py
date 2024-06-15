def load_files(spark, file_path, header, inferSchema):

    try:
        if file_format == 'parquet':
            df = spark.read.parquet(file_path)
        elif file_format == 'csv':
            df = spark.read.csv(file_path, header=header, inferSchema=inferSchema)

    except Exception as e:
        print(f'Error while loading file in DataFrame -- {e}')
    else:
        return df