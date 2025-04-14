import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.types import IntegerType
from pyspark.ml import Pipeline

from datetime import datetime, date, time, timezone, timedelta
from math import ceil

start_date = date(2021, 12, 25)
end_date = date(2022, 9, 25)
interval = 60
interval_m = 2
K = 10

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e70da526-f9f6-4020-aae0-96d99f24814a"),
    final_pre=Input(rid="ri.vector.main.execute.8359711f-3145-4cb8-92c6-ca6fd969fd75")
)
# convert to ltmle format data
def final_AY(final_pre):
    df = final_pre

    # calculate vax_to_treatment (A), vax_to_long_covid (Y)
    AY_map = {'vax_to_treatment': 'treatment_date', 'vax_to_long': 'long_covid_date'}
    for c, v in AY_map.items():
        df = df.withColumn(c, F.months_between(F.col(v), F.col('t0'))) # depends on the interval we chose

    ##### Construct Anodes, Ynodes
    for k in range(1, K+1): # go through each time block
        df = df.withColumn('A' + str(k), when((F.col('vax_to_treatment') <= interval_m * k + 1) & (F.col('vax_to_treatment') > interval_m * (k-1) + 1), 1).otherwise(0))

    for k in range(1, K+1): # rewrite this loop due to the order of columns
        df = df.withColumn('Y' + str(k), when((F.col('vax_to_long') <= interval_m * k + 1) & (F.col('vax_to_long') >= 0) , 1).otherwise(0))

    drop_c = [x for x in df.columns if 'date' in x]
    # df = df.drop(*drop_c)
    
    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cb526597-cbe2-4b89-b829-378b98c3cac8"),
    add_reinfection=Input(rid="ri.vector.main.execute.b9c6b1f0-0cbe-434a-83d0-9bd42e53a72c")
)
def get_covid_date(add_reinfection):
    df = add_reinfection
    # convert before baseline (t0) to null
    covid_dates_l = [x for x in df.columns if x!= 'person_id' and x!= 't0']
    print(covid_dates_l)

    for c in covid_dates_l:
        df = df.withColumn(c, when((F.col(c) <= F.col('t0')), None).otherwise(F.col(c)))

    # smallest covid during the window, and three 6 month-windows
    df = df.withColumn('treatment_date', F.least('covid_index', '1_reinfect_date', '2_reinfect_date', '3_reinfect_date', '4_reinfect_date'))
    df = df.select(['person_id', 'treatment_date'])
    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.ad809463-78de-44f2-a451-8b0d6d1b2360"),
    Add_date_df=Input(rid="ri.foundry.main.dataset.b6e90ff2-cc12-4443-8824-b9fce92d5cb4")
)
def get_t0(Add_date_df):
    df = Add_date_df
    # convert outside window dates to null
    vax_dates_l = [x for x in df.columns if '_vax_date' in x]

    for c in vax_dates_l:
        df = df.withColumn(c, when((F.col(c) < start_date)| (F.col(c) > end_date), None).otherwise(F.col(c)))

    # smallest vaccination during the window, and three 6 month-windows
    df = df.withColumn('t0', F.least('1_vax_date', '2_vax_date', '3_vax_date', '4_vax_date', '5_vax_date'))

    # extract info of vaccine type
    date_to_type = {'1_vax_date': '1_vax_type', '2_vax_date': '2_vax_type', '3_vax_date': '3_vax_type', '4_vax_date': '4_vax_type', '5_vax_date': '5_vax_type'}
    for c in vax_dates_l:
        df = df.withColumn(date_to_type[c], when(F.col('t0')!=F.col(c), None).otherwise(F.col(date_to_type[c])))

    df = df.withColumn('baseline_type', F.least('1_vax_type', '2_vax_type', '3_vax_type', '4_vax_type', '5_vax_type'))

    df = df.select(['person_id', 'covid_index', 't0', 'baseline_type'])
    return df

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.9ab54747-baaa-4d06-8877-36c821299de1"),
    Add_date_df=Input(rid="ri.foundry.main.dataset.b6e90ff2-cc12-4443-8824-b9fce92d5cb4"),
    get_t0=Input(rid="ri.vector.main.execute.ad809463-78de-44f2-a451-8b0d6d1b2360"),
    reinfection_wide=Input(rid="ri.vector.main.execute.621f779b-9d53-4de1-bac1-29b6881a8140")
)
from itertools import product
def pre_baseline(get_t0, reinfection_wide, Add_date_df):
    vax_dates_l = [x for x in Add_date_df.columns if '_vax_date' in x]
    covid_dates = [x for x in reinfection_wide.columns if x!='person_id'] + ['covid_index']

    df = get_t0.join(Add_date_df, on = ['person_id', 'covid_index']).join(reinfection_wide, how = 'left', on = 'person_id')

    # initialize 3 pre_baseline_vax indicators
    all_combi = list(product(['vax', 'covid'], ['6m', '12m', 'prior']))
    pre_baseline = ['num_' + x + '_' + y for x, y in all_combi]
    for c in pre_baseline:
        df = df.withColumn(c, F.lit(0))

    # TODO: can refactor in the future
    for c in vax_dates_l:
        df = df.withColumn('num_vax_6m', when((F.col(c) < F.col('t0')) & (F.col(c) > F.add_months(F.col('t0'), -6)), F.col('num_vax_6m')+1).otherwise(F.col('num_vax_6m')))
        df = df.withColumn('num_vax_12m', when((F.col(c) < F.add_months(F.col('t0'), -6)) & (F.col(c) > F.add_months(F.col('t0'), -12)), F.col('num_vax_12m')+1).otherwise(F.col('num_vax_12m')))
        df = df.withColumn('num_vax_prior', when(F.col(c) < F.add_months(F.col('t0'), -12), F.col('num_vax_prior')+1).otherwise(F.col('num_vax_prior')))
    # df = df.drop(*vax_dates_l)

    for c in covid_dates:
        df = df.withColumn('num_covid_6m', when((F.col(c) < F.col('t0')) & (F.col(c) > F.add_months(F.col('t0'), -6)), F.col('num_covid_6m')+1).otherwise(F.col('num_covid_6m')))
        df = df.withColumn('num_covid_12m', when((F.col(c) < F.add_months(F.col('t0'), -6)) & (F.col(c) > F.add_months(F.col('t0'), -12)), F.col('num_covid_12m')+1).otherwise(F.col('num_covid_12m')))
        df = df.withColumn('num_covid_prior', when(F.col(c) < F.add_months(F.col('t0'), -12), F.col('num_covid_prior')+1).otherwise(F.col('num_covid_prior')))
    # df = df.drop(*covid_dates)

    return df
        
    
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.621f779b-9d53-4de1-bac1-29b6881a8140"),
    reinfection=Input(rid="ri.foundry.main.dataset.e18f7c91-fdd5-4639-acfb-c67e636dbec1")
)
from pyspark.sql.types import StringType
def reinfection_wide(reinfection):
    df = reinfection
    df = df.groupBy("person_id").pivot("reinfection_number").agg(F.first("reinfection_date").alias("reinfect_date"))
    df = df.select([F.col(c).alias(c+'_reinfect_date') if c != 'person_id' else 'person_id' for c in df.columns])

    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.2847c9a0-76ea-43d7-9f26-ab39ac304f99"),
    add_reinfection=Input(rid="ri.vector.main.execute.b9c6b1f0-0cbe-434a-83d0-9bd42e53a72c")
)
def unnamed(add_reinfection):
    df = add_reinfection
    df = df.withColumn('covid_to_long', F.months_between(F.col('long_covid_date'), F.col('covid_index')))
    df.select('covid_to_long').summary().show()
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c0d1eb99-b1bc-4437-b9e7-7017cae3513c"),
    final_AY=Input(rid="ri.foundry.main.dataset.e70da526-f9f6-4020-aae0-96d99f24814a")
)
def unnamed_1(final_AY):
    df = final_AY
    df = df.withColumn('treatment_to_long', F.months_between(F.col('long_covid_date') , F.col('treatment_date'))/interval_m)
    df = df.filter(df.vax_to_treatment > 0)
    df.select('vax_to_treatment', 'vax_to_long', 'treatment_to_long').summary().show()
    df = df.select('t0', 'treatment_date', 'long_covid_date', 'vax_to_treatment', 'vax_to_long', 'treatment_to_long')
    return df

