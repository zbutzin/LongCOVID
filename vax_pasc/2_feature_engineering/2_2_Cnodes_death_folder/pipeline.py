import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.types import IntegerType
from pyspark.ml import Pipeline

from datetime import datetime, date, time, timezone, timedelta
from math import ceil

start_date = date(2021, 12, 25)
# end_date = date(2022, 12, 31)
interval = 60
interval_m = 2
K = 10

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cff588b0-76bc-4f8d-84d7-24dfe29934e2"),
    death=Input(rid="ri.foundry.main.dataset.d8cc2ad4-215e-4b5d-bc80-80ffb3454875"),
    final_AY=Input(rid="ri.foundry.main.dataset.e70da526-f9f6-4020-aae0-96d99f24814a")
)
def C_nodes(death, final_AY):
    df = final_AY.select(['person_id', 't0'])
    death = death.select(['person_id', 'death_date']).groupBy('person_id').agg(F.max('death_date').alias('death_date'))
    df = df.join(death, on = 'person_id', how = 'left')

    # vax_to_death
    df = df.withColumn('end_date', F.to_date(F.lit('03-07-2024'), "MM-dd-yyyy"))
    df = df.withColumn('vax_to_death', F.months_between(F.col('death_date'), F.col('t0')))
    df = df.withColumn('vax_to_today', F.months_between(F.col('end_date'), F.col('t0')))
    for k in range(1, K+1): # go through each time block
        df = df.withColumn('C' + str(k), when((F.col('vax_to_death') <= interval_m * k + 1) | (F.col('vax_to_today') <= interval_m * k + 1), 1).otherwise(0))
    return df

    
    

