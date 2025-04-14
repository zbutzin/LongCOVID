import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.types import IntegerType
from pyspark.ml import Pipeline
from math import *

from datetime import datetime, date, time, timezone, timedelta
import math
from math import ceil

def calc_RR(n, x):
    n1, n2 = n
    x1, x2 = x
    p1, p2 = x1/n1, x2/n2

    RR = p1/p2
    sd = math.sqrt(1/x1 - 1/n1 + 1/x2 - 1/n2)
    # sd = math.sqrt(((n1-x1)/x1)/n1+((n2-x2)/x2)/n2)

    lw, up = log(RR) - 1.96*sd, log(RR) + 1.96*sd

    return (RR, exp(lw), exp(up))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2fad38c1-ae0b-496a-98a7-4abc4296f18a"),
    Final=Input(rid="ri.foundry.main.dataset.a269ae79-4154-4849-8788-0210fc3fc7e8")
)
# clean_and_impute (2f95083e-5bf5-4f45-b9ce-ce8513d97370): v6
def clean_and_impute(Final, covariates):
    df = Final
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
    Output(rid="ri.foundry.main.dataset.c702a53b-86af-457c-84f7-d376c4f41aa1"),
    Final=Input(rid="ri.foundry.main.dataset.a269ae79-4154-4849-8788-0210fc3fc7e8")
)
# clean_and_impute (2f95083e-5bf5-4f45-b9ce-ce8513d97370): v6
def clean_negative_exposure(Final, covariates):
    df = Final
    covariates_names = list(covariates.select('names').toPandas()['names'])
    all_columns = covariates_names + ['azithromycin_indicator', 'Long_COVID_diagnosis_post_covid_indicator']
    df = df.select(*all_columns)
    # df = df.drop('region')

    # rename some columns
    to_rename = [x for x in df.columns if '-' in x]
    renamed = lambda x: x.replace('-', '_')
    for x in to_rename:
        df = df.withColumnRenamed(x, renamed(x))

    # get columns which contain null values
    null_df = df.select([c for c in df.columns if df.filter(F.col(c).isNull()).count() > 0])
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
    Output(rid="ri.vector.main.execute.5905c8d9-9ada-4299-b6cb-d3224e2099b6"),
    Final=Input(rid="ri.foundry.main.dataset.a269ae79-4154-4849-8788-0210fc3fc7e8")
)
# clean_and_impute (2f95083e-5bf5-4f45-b9ce-ce8513d97370): v6
def clean_negative_outcome(Final, covariates):
    df = Final
    covariates_names = list(covariates.select('names').toPandas()['names'])
    all_columns = covariates_names + ['SSRI_Indicator', 'fracture_indicator']
    df = df.select(*all_columns)
    # df = df.drop('region')

    # rename some columns
    to_rename = [x for x in df.columns if '-' in x]
    renamed = lambda x: x.replace('-', '_')
    for x in to_rename:
        df = df.withColumnRenamed(x, renamed(x))

    # get columns which contain null values
    null_df = df.select([c for c in df.columns if df.filter(F.col(c).isNull()).count() > 0])
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
    Output(rid="ri.foundry.main.dataset.c6acaa19-8a1f-4969-9eca-97ee00c8ae37"),
    clean_and_impute=Input(rid="ri.foundry.main.dataset.2fad38c1-ae0b-496a-98a7-4abc4296f18a")
)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
from scipy.stats import pearsonr
def correlation(clean_and_impute):
    df = clean_and_impute
    exposure, outcome = 'SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator'
    covariates = [x for x in df.columns if x!=exposure and x!=outcome]
    data = []
    exposure_array = list(df.select(exposure).toPandas()[exposure])
    outcome_array = list(df.select(outcome).toPandas()[outcome])
    for x in covariates:
        # ce = df.stat.corr(x, exposure)
        x_array = list(df.select(x).toPandas()[x])
        ce, pe = pearsonr(x_array, exposure_array)
        # co = df.stat.corr(x, outcome)
        co, po = pearsonr(x_array, outcome_array)
        data.append((x, ce, pe, co, po))

    final = pd.DataFrame(data, columns =['feature', 'exposure_corr', 'exposure_p', 'outcome_corr', 'outcome_p'], dtype = float) 
    return final

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.16a1526b-7318-40f1-a0ef-0af19e22822d"),
    dosage_type_cleaned=Input(rid="ri.foundry.main.dataset.774e3e38-a3aa-439b-9686-79347a3203f2")
)
def dosage_low(dosage_type_cleaned):
    df = dosage_type_cleaned
    target_dosage = 60 # 20, 40, 60
    df = df.filter((F.col('fluoxetine_dosage') == 10) | (F.col('fluoxetine_dosage') == target_dosage)) # dosage == 10 (low) or 20
    new_column = 'dosage_' + str(target_dosage) + '_low_ind'
    df = df.withColumn(new_column, when(df['fluoxetine_dosage']==target_dosage, 1).otherwise(0))
    df = df.drop('fluoxetine_dosage', 'fluoxetine_indicator')
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5dbb8c35-ca13-4706-a4cf-06bb970ebff4"),
    dosage_type_cleaned=Input(rid="ri.foundry.main.dataset.774e3e38-a3aa-439b-9686-79347a3203f2")
)
def dosage_none(dosage_type_cleaned):
    df = dosage_type_cleaned
    target_dosage = 60 # 10, 20, 40, 60
    df = df.filter((F.col('fluoxetine_dosage') == target_dosage) | (F.col('fluoxetine_indicator') == 0)) # dosage == 10 or no fluoxetine
    new_column = 'dosage_' + str(target_dosage) + '_ind'
    df = df.withColumn(new_column, when(df['fluoxetine_dosage']==target_dosage, 1).otherwise(0))
    df = df.drop('fluoxetine_dosage', 'fluoxetine_indicator')

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.774e3e38-a3aa-439b-9686-79347a3203f2"),
    Final=Input(rid="ri.foundry.main.dataset.a269ae79-4154-4849-8788-0210fc3fc7e8")
)
def dosage_type_cleaned(Final, covariates):
    df = Final
    covariates_names = list(covariates.select('names').toPandas()['names'])
    all_columns = covariates_names + ['fluoxetine_indicator', 'fluoxetine_dosage', 'Long_COVID_diagnosis_post_covid_indicator']
    df = df.select(*all_columns)
    
    # rename some columns
    to_rename = [x for x in df.columns if '-' in x]
    renamed = lambda x: x.replace('-', '_')
    for x in to_rename:
        df = df.withColumnRenamed(x, renamed(x))

    # get columns which contain null values
    null_df = df.select([c for c in df.columns if df.filter(F.col(c).isNull()).count() > 0])
    print(null_df.columns)
    dic = df.select([F.avg(c).alias(c) for c in null_df.columns]).first().asDict()
    df = df.fillna(dic)

    # make indicators for different dosage levels
    # Common dosage - 10, 20, 40, 60 (include separate estimate for missing)
    # Exclude less common dosages (likely errors)
    # dosage v.s none, dosage v.s low dosage
    
    
    
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aa9a922b-6cc2-48b5-a538-3d56029bd395"),
    Final=Input(rid="ri.foundry.main.dataset.a269ae79-4154-4849-8788-0210fc3fc7e8")
)
def drug_type_cleaned(covariates, Final):
    df = Final
    covariates_names = list(covariates.select('names').toPandas()['names'])
    drug_types = ['fluoxetine_indicator', 'sertraline_indicator', 'paroxetine_indicator', 'fluvoxamine_indicator', 'citalopram_indicator', 'vilazodone_indicator', 'escitalopram_indicator']
    all_columns = covariates_names + ['SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator'] + drug_types
    df = df.select(*all_columns)
    # df = df.drop('region')

    # rename some columns
    to_rename = [x for x in df.columns if '-' in x]
    renamed = lambda x: x.replace('-', '_')
    for x in to_rename:
        df = df.withColumnRenamed(x, renamed(x))

    # get columns which contain null values
    null_df = df.select([c for c in df.columns if df.filter(F.col(c).isNull()).count() > 0])
    print(null_df.columns)
    dic = df.select([F.avg(c).alias(c) for c in null_df.columns]).first().asDict()
    df = df.fillna(dic)
    
    return df
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.8ff303c4-8545-4012-8714-78b8c4ce7b56"),
    AY=Input(rid="ri.foundry.main.dataset.c4e22893-6d13-4824-8f6c-3bf19a1f26b5")
)
def sample_sizes(AY):
    df = AY
    df = df.filter(df.Long_COVID_diagnosis_post_covid_indicator==1)
    column_sums = df.agg(*[F.sum(column).alias(column) for column in df.columns])
    return(column_sums)
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.22511900-e923-4411-b91d-d9ea4fa6d52d"),
    AY=Input(rid="ri.foundry.main.dataset.c4e22893-6d13-4824-8f6c-3bf19a1f26b5")
)
def unadjusted(AY):
    df = AY
    df = df.groupBy("SSRI_Indicator").agg(count("*").alias("n"), \
        F.sum("Long_COVID_diagnosis_post_covid_indicator").alias('x'),\
        F.avg("Long_COVID_diagnosis_post_covid_indicator").alias("proportion")
        )
    drug_types = ['fluoxetine_indicator', 'sertraline_indicator', 'paroxetine_indicator', 'fluvoxamine_indicator', 'citalopram_indicator', 'vilazodone_indicator', 'escitalopram_indicator']
    # (treatment/control)
    result = calc_RR((124928, 381975), (3334, 8284))
    print(result)
    return df

