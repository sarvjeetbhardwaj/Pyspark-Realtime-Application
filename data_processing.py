from pyspark.sql.functions import *
from pyspark.sql.types import *

def data_clean(df1, df2):

    try:
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

        #df_presc_sel = df_presc_sel.select([count(when (isnan(c)|col(c).isNull(),c)).alias(c) for c in df_presc_sel.columns])
        #Check for null values first 
        df_presc_sel = df_presc_sel.dropna(subset='pres_id')
        df_presc_sel = df_presc_sel.dropna(subset='drug_name')

        mean_claim_amount = df_presc_sel.select(mean(df_presc_sel.claim_count)).collect()[0][0]
        df_presc_sel = df_presc_sel.fillna({'claim_count': mean_claim_amount})

    except Exception as e:
        print(f'exception occured while cleaning the file, {e}')
    else:
        return df_city_sel, df_presc_sel
