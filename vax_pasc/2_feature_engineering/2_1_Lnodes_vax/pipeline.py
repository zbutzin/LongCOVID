import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.types import IntegerType
from pyspark.ml import Pipeline

from datetime import datetime, date, time, timezone, timedelta
from math import ceil

start_date = date(2021, 12, 25)
end_date = date(2022, 5, 25)
interval = 60
interval_m = 2
K = 10

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bf1c589f-19ad-4bbe-a9b9-94ff96d5605a"),
    final_AY=Input(rid="ri.foundry.main.dataset.e70da526-f9f6-4020-aae0-96d99f24814a")
)
def Final_Lvax(final_AY):
    df = final_AY
    new_col_map = {'vax_to_covid': 'covid_index', 'vax_to_1reinfect': '1_reinfect_date', 'vax_to_2reinfect': '2_reinfect_date', 'vax_to_3reinfect': '3_reinfect_date',
                    'vax_to_4reinfect': '4_reinfect_date', 
                    'vax_to_1vax': '1_vax_date', 'vax_to_2vax': '2_vax_date', 'vax_to_3vax': '3_vax_date', 'vax_to_4vax': '4_vax_date', 'vax_to_5vax': '5_vax_date'}
    vax_l, covid_l = [], []
    for c, v in new_col_map.items():
        df = df.withColumn(c, F.months_between(F.col(v), F.col('t0'))) # depends on the interval we chose
        if 'covid' in c or 'reinfect' in c:
            covid_l.append(c)
        else:
            vax_l.append((c, v))

    # construct L_vax for all positive vax_to...
    date_to_type = {'1_vax_date': '1_vax_type', '2_vax_date': '2_vax_type', '3_vax_date': '3_vax_type', '4_vax_date': '4_vax_type', '5_vax_date': '5_vax_type'}
    for c, v in vax_l:
        for k in range(1, K+1): # go through each time block
            L_name = 'L' + str(k) + 'vax'
            df = df.withColumn(L_name, when((F.col(c) <= interval_m * k + 1) & (F.col(c) > interval_m * (k-1) + 1), 1).otherwise(0))
            df = df.withColumn('L' + str(k) + 'vax_type', when(F.col(L_name)==1, F.col(date_to_type[v])).otherwise(None))

    # incoporate reinfection into A nodes
    for c in covid_l:
        for k in range(1, K+1): # go through each time block
            A_node = 'A' + str(k)
            df = df.withColumn(A_node, when((F.col(c) <= interval_m * k + 1) & (F.col(c) > interval_m * (k-1) + 1) & (F.col(c) != F.col('vax_to_treatment')), 1).otherwise(F.col(A_node)))
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4d171d5f-1ffb-48d3-813e-cb95e4ccb70f"),
    Final_Lvax=Input(rid="ri.foundry.main.dataset.bf1c589f-19ad-4bbe-a9b9-94ff96d5605a"),
    L_LTFU=Input(rid="ri.foundry.main.dataset.c9dd561f-d264-4c60-b631-669337b117c7")
)
def Final_withL(Final_Lvax, L_LTFU):
    df = Final_Lvax.join(L_LTFU, on='person_id', how='inner')
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c9dd561f-d264-4c60-b631-669337b117c7"),
    visit_interval=Input(rid="ri.foundry.main.dataset.09ac5fe6-b996-4682-b1b7-0f0725b4e5b6")
)
def L_LTFU(visit_interval):
    df = visit_interval.filter(F.col('vax_to_visit') <= K)
    # pivot and construct L_LOCF nodes
    df = df.withColumn('ind', F.lit(1))
    df = df.groupBy('person_id').pivot('vax_to_visit').sum('ind')
    df = df.fillna(0)

    # change column names
    updated_columns = [F.col(col_name).alias("L"+ str(col_name) + "LTFU") if col_name != 'person_id' else 'person_id' for col_name in df.columns]
    df = df.select(*updated_columns)
    return df
    

