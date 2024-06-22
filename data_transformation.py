from udfs import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

def data_report(df_city_sel, df_presc_sel):
    df_city_split = df_city_sel.withColumn('zipcount', column_split_count(df_city_sel.zips))

    df_presc_group = df_presc_sel.groupBy(df_presc_sel.presc_state, df_presc_sel.presc_city).agg(countDistinct('pres_id').alias('pres_count'),sum('claim_count').alias('claim_sum'))

    df_city_join = df_city_split.join(df_presc_group, (df_city_sel.state_id == df_presc_group.presc_state) & 
                                      (df_city_sel.City == df_presc_group.presc_city ), 'inner') 

    df_final = df_city_join.select('City', 'State_Name', 'Country_Name',  'population', 'zipcount', 'pres_count')

    return df_final

def data_report2(df_presc_sel):
    spec = Window.partitionBy('presc_state').orderBy(col('claim_count').desc())
    df_pres_final = df_presc_sel.select('pres_id', 'presc_fullname', 'presc_state', 'claim_count', 'years_of_exp', 'total_day_supply').filter((col('years_of_exp')>=20) & (col('years_of_exp')<=50)).\
                    withColumn('dense_rank', dense_rank().over(spec)).filter(col('dense_rank') <=5)
                    
    return df_pres_final

