## This function validates whether spark object has been created or not ###

def get_current_date(spark):
    try:
        output = spark.sql("""Select current_date""")
    except Exception as e:
        print(f'An error occured while validating spark object with get_current_date :: {e}')
    else:
        print('spark object Validated')
