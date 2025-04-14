import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.classification import LogisticRegression
import matplotlib.pyplot as plt

from datetime import datetime, date, time, timezone, timedelta
from math import ceil

start_date = date(2021, 12, 25)
# end_date = date(2022, 12, 31)

@transform_pandas(
    Output(rid="ri.vector.main.execute.fc3842fb-9271-41ba-b726-e7514652dba0")
)
from pyspark.sql.types import *
def covariate_list_v139():
    schema = StructType([StructField("name", StringType(), True)])
    return spark.createDataFrame([["BMI_max_observed_or_calculated_before_covid"],["TOBACCOSMOKER_before_covid_indicator"],["OBESITY_before_covid_indicator"],["DIABETESUNCOMPLICATED_before_covid_indicator"],["CHRONICLUNGDISEASE_before_covid_indicator"],["HYPERTENSION_before_covid_indicator"],["DEPRESSION_before_covid_indicator"],["SYSTEMICCORTICOSTEROIDS_before_covid_indicator"],["ASTHMA_before_covid_indicator"],["data_partner_id"],["cdm_name"],["percent_income_below_poverty"],["social_community_score"],["number_of_visits_before_covid"],["gender_concept_name"],["age_at_covid"],["race_ethnicity"],["observation_period_before_covid"],["OTHERIMMUNOCOMPROMISED_before_or_day_of_covid_indicator"]], schema=schema)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1d1a5f5a-18dc-490a-8f5f-8ffb3423a57a"),
    ISC_CommonFacts_COVID_Patient_Summary_Table_v163=Input(rid="ri.foundry.main.dataset.6a150518-42ce-498a-9d3f-7073df5cb7b7"),
    covariate_list_v139=Input(rid="ri.vector.main.execute.fc3842fb-9271-41ba-b726-e7514652dba0")
)
def covariate_list_v145(covariate_list_v139, ISC_CommonFacts_COVID_Patient_Summary_Table_v163):
    covariates = covariate_list_v139
    df = ISC_CommonFacts_COVID_Patient_Summary_Table_v163

    spark = SparkSession.builder.appName('sparkdf').getOrCreate()
    column = ["converted_column"]
    data = [[x] for x in df.columns]
    new_df = spark.createDataFrame(data, column)

    second_df = covariate_list_v139.withColumn('name2', F.split(covariate_list_v139['name'], '_covid')[0]).select('name2')
    result_df = new_df.crossJoin(second_df) 
    
    result_df = result_df.filter(F.col("converted_column").contains(F.col("name2")))
    return result_df

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.4adb809c-17cd-4b92-a5e7-5d2bab05feb7"),
    ISC_CommonFacts_COVID_Patient_Summary_Table_v163=Input(rid="ri.foundry.main.dataset.6a150518-42ce-498a-9d3f-7073df5cb7b7"),
    covariate_list_v145=Input(rid="ri.foundry.main.dataset.1d1a5f5a-18dc-490a-8f5f-8ffb3423a57a")
)
def covariates( ISC_CommonFacts_COVID_Patient_Summary_Table_v163, covariate_list_v145):
    covariate_list = covariate_list_v145
    t = ISC_CommonFacts_COVID_Patient_Summary_Table_v163
    l = list(covariate_list.select('converted_column').toPandas()['converted_column'])

    # add sex
    # TODO: missing percent_income_below_poverty, social_community_score
    l = l + ['person_id', 'sex']
    final = t.select(l)
    return(final)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d8070d34-be0f-4109-a394-a6f4e1820f32"),
    add_date_df=Input(rid="ri.foundry.main.dataset.b6e90ff2-cc12-4443-8824-b9fce92d5cb4"),
    covariates=Input(rid="ri.vector.main.execute.4adb809c-17cd-4b92-a5e7-5d2bab05feb7")
)
def preprocessed(covariates, add_date_df):
    # filter the person_id in main table
    df = covariates
    df_sub = add_date_df.select('person_id')
    df = df_sub.join(df, on='person_id', how='left')

    ##### Imputing ########
    print("Imputing")
    # make new indicator for missingness
    null_df = df.select([c for c in df.columns if df.filter(F.col(c).isNull()).count() > 0])
    ind_df = df.select([when(col(c).isNull(), 1).otherwise(0).alias(c+'_ind') for c in null_df.columns])

    # impute the value with mean
    dic = df.select([F.avg(c).alias(c) for c in null_df.columns]).first().asDict()
    df = df.fillna(dic)

    ##### Encode string variables ########
    print("Encoding")
    # convert data_partner_id to StringType
    df = df.withColumn("data_partner_id", df["data_partner_id"].cast(StringType()))

    # column that has type string - get dummies
    s_l = [x[0] for x in df.dtypes if x[1] == 'string']
    s_l.remove('person_id')
    print(s_l)

    for s in s_l:
        df = df.withColumn(s, F.lower(F.regexp_replace(df[s], "[^A-Za-z_0-9]", "_" )))
    df_factor = pd.get_dummies(df.select(s_l).toPandas(), drop_first = True)
    spark = SparkSession.builder.appName("pandas to spark").getOrCreate()
    df_factor = spark.createDataFrame(df_factor)
    df = df.drop(*tuple(s_l))

    # join them
    w = Window.orderBy(F.lit(1))
    df1 = ind_df.withColumn("rn", F.row_number().over(w)-1)
    df2 = df.withColumn("rn", F.row_number().over(w)-1)
    df3 = df_factor.withColumn("rn", F.row_number().over(w)-1)
    df = df1.join(df2,["rn"]).join(df3, ["rn"]).drop('rn')

    return(df)
    

