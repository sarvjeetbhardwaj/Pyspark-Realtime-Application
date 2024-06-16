from udfs import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def data_report(df_city_sel, df_presc_sel):
    df_city_split = df_city_sel.withColumn('zipcount', column_split_count(df_city_sel.zips))

    df_presc_group = df_presc_sel.groupBy(df_presc_sel.presc_state, df_presc_sel.presc_city).agg(countDistinct('pres_id').alias('pres_count'),sum('claim_count').alias('claim_sum'))

    df_city_join = df_city_split.join(df_presc_group, (df_city_sel.state_id == df_presc_group.presc_state) & 
                                      (df_city_sel.City == df_presc_group.presc_city ), 'inner') 

    df_final = df_city_join.select('City', 'State_Name', 'Country_Name',  'population', 'zipcount', 'pres_count')

    return df_final

