

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b3adf548-df4b-4b44-aba4-a1e3712c7fb7"),
    Data_partners_filtered=Input(rid="ri.foundry.main.dataset.112bcdb0-cebe-44b8-a4c1-94a09bcdf28f"),
    covariates=Input(rid="ri.vector.main.execute.c156981a-b903-460e-85c2-b6efb3922aa1")
)
# clean_and_impute (2f95083e-5bf5-4f45-b9ce-ce8513d97370): v8
def cleaned(Data_partners_filtered, covariates):
    df = Data_partners_filtered
    covariates_names = list(covariates.select('names').toPandas()['names'])
    all_columns = covariates_names + ['SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator']
    df = df.select(*all_columns)
    # df = df.drop('region')

    # rename some columns
    to_rename = [x for x in df.columns if '-' in x]
    renamed = lambda x: x.replace('-', '_')
    for x in to_rename:
        df = df.withColumnRenamed(x, renamed(x))

    # get columns which contain null values
    null_df = df.select([c for c in df.columns if df.filter(F.col(c).isNull()).count() > 0])

    for c in null_df.columns:
        df = df.withColumn(c+'_ind', when(col(c).isNull(), 1).otherwise(0))

    print(null_df.columns)
    dic = df.select([F.avg(c).alias(c) for c in null_df.columns]).first().asDict()
    df = df.fillna(dic)
    
    return df

#################################################
## Global imports and functions included below ##
#################################################

import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.types import IntegerType
from pyspark.ml import Pipeline
from math import *

from datetime import datetime, date, time, timezone, timedelta
from math import ceil

def calc_RR(n, x):
    n1, n2 = n
    x1, x2 = x
    p1, p2 = x1/n1, x2/n2

    RR = p1/p2
    sd = math.sqrt(((n1-x1)/x1)/n1+((n2-x2)/x2)/n2)

    lw, up = log(RR) - 1.96*sd, log(RR) + 1.96*sd

    return (RR, exp(lw), exp(up))

@transform_pandas(
    Output(rid="ri.vector.main.execute.c156981a-b903-460e-85c2-b6efb3922aa1")
)
from pyspark.sql.types import *
def covariates():
    schema = StructType([StructField("names", StringType(), True)])
    return spark.createDataFrame([["number_of_visits_before_covid"],["month_number_14"],["visits_per_month"],["age_at_covid"],["BMI_max_observed_or_calculated_before_or_day_of_covid"],["CHRONICLUNGDISEASE_before_or_day_of_covid_indicator"],["DIABETESUNCOMPLICATED_before_or_day_of_covid_indicator"],["DIABETESCOMPLICATED_before_or_day_of_covid_indicator"],["OBESITY_before_or_day_of_covid_indicator"],["OTHERIMMUNOCOMPROMISED_before_or_day_of_covid_indicator"],["TOBACCOSMOKER_before_or_day_of_covid_indicator"],["SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator"],["HYPERTENSION_before_or_day_of_covid_indicator"],["number_of_COVID_vaccine_doses_before_or_day_of_covid"],["sex_FEMALE"],["sex_MALE"],["sex_No_matching_concept"],["sex_UNKNOWN"],["sex_OTHER"],["race_ethnicity_White_Non-Hispanic"],["race_ethnicity_Black_or_African_American_Non-Hispanic"],["race_ethnicity_Hispanic_or_Latino_Any_Race"],["race_ethnicity_Unknown"],["race_ethnicity_Asian_Non-Hispanic"],["race_ethnicity_Other_Non-Hispanic"],["cdm_name_OMOP"],["cdm_name_PCORNET"],["cdm_name_TRINETX"],["cdm_name_ACT"],["cdm_name_OMOP_PEDSNET"],["region_2"],["region_4"],["region_5"],["region_0"],["region_9"],["region_8"],["region_N"],["region_1"],["region_6"],["region_3"],["region_7"],["post_COVID_visit_indicator"],["race_ethnicity_American_Indian_or_Alaska_Native_Non-Hispanic"],["race_ethnicity_Native_Hawaiian_or_Other_Pacific_Islander_Non-Hispanic"],["sdi_score"],["poverty_rate"],["bd_indicator"],["depression_severity_Missing"],["depression_severity_Mild"],["depression_severity_Severe"],["depression_severity_Moderately"],["HEARTFAILURE_before_or_day_of_covid_indicator"],["month_number_1"],["month_number_2"],["month_number_3"],["month_number_4"],["month_number_5"],["month_number_6"],["month_number_7"],["month_number_8"],["month_number_9"],["month_number_10"],["month_number_11"],["month_number_12"],["month_number_13"],["CCI_score_up_through_index_date"]], schema=schema)

