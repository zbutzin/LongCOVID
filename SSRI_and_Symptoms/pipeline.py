from pyspark.sql.functions import col, when, avg, lit, row_number, lower, regexp_replace, median
from pyspark.sql.types import IntegerType, StringType
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

health_util_col = ['number_of_visits_before_covid', 'visits_per_month']

covariates = ['sex', 'age_at_covid', 'cdm_name', 'state', 'county', 'BMI_max_observed_or_calculated_before_or_day_of_covid','TOBACCOSMOKER_before_or_day_of_covid_indicator',
'OBESITY_before_or_day_of_covid_indicator',
'DIABETESCOMPLICATED_before_or_day_of_covid_indicator',
'DIABETESUNCOMPLICATED_before_or_day_of_covid_indicator',
'CHRONICLUNGDISEASE_before_or_day_of_covid_indicator',
'CONGESTIVEHEARTFAILURE_before_or_day_of_covid_indicator',
'HEARTFAILURE_before_or_day_of_covid_indicator',
'HYPERTENSION_before_or_day_of_covid_indicator',
'SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator', 'anxiety', 'antipsychotic_medications', 'benzodiazepine', 
'OTHERIMMUNOCOMPROMISED_before_or_day_of_covid_indicator', 'number_of_COVID_vaccine_doses_before_or_day_of_covid',
'CCI_score_up_through_index_date', 'months'] + health_util_col

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.98bbe99e-ee9b-4c9a-904a-0a98ad7ce031"),
    Select_columns=Input(rid="ri.foundry.main.dataset.7da712ab-1435-49d5-ac3c-d02ab186882a")
)
def Preprocess(Select_columns):
    df = Select_columns.drop(*['state', 'county'])
    # impute with median: age BMI, CCI
    null_df = df.select([c for c in df.columns if df.filter(col(c).isNull()).count() > 0])
    print(null_df.columns) 
    ind_df = df.select([when(col(c).isNull(), 1).otherwise(0).alias(c+'_ind') for c in null_df.columns])
    dic = df.select([median(c).alias(c) for c in null_df.columns]).first().asDict()
    df = df.fillna(dic)

    # one hot encoder
    s_l = [x[0] for x in df.dtypes if x[1] == 'string']
    print(s_l)

    for s in s_l:
        df = df.withColumn(s, lower(regexp_replace(df[s], "[^A-Za-z_0-9]", "_" )))
    df_factor = pd.get_dummies(df.select(s_l).toPandas(), drop_first = True)
    spark = SparkSession.builder.appName("pandas to spark").getOrCreate()
    df_factor = spark.createDataFrame(df_factor)
    df = df.drop(*tuple(s_l))

    # join them
    w = Window.orderBy(lit(1))
    df1 = ind_df.withColumn("rn", row_number().over(w)-1)
    df2 = df.withColumn("rn", row_number().over(w)-1)
    df3 = df_factor.withColumn("rn", row_number().over(w)-1)
    df = df1.join(df2,["rn"]).join(df3, ["rn"]).drop('rn')
    return(df)
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7da712ab-1435-49d5-ac3c-d02ab186882a"),
    Final_table_2=Input(rid="ri.foundry.main.dataset.7ced97de-8cd8-4714-904e-09d285c32513")
)
import re
def Select_columns(Final_table_2):
    df = Final_table_2
    # all_symptoms = [x for x in df.columns if 'symptom' in x]
    include_list = ["parent_", "category_"]
    pattern = re.compile("|".join(map(re.escape, include_list)))
    outcome_columns = [x for x in df.columns if pattern.search(x)]
    
    race_col = [x for x in df.columns if 'race_ethnicity_' in x]
    depression_sev_col = [x for x in df.columns if 'depression_severity_' in x]

    select_columns = covariates + race_col + depression_sev_col + outcome_columns + ['SSRI', 'two_visits_post_covid']

    df = df.select(*select_columns)
    return(df)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.22323bf0-5b79-4c5e-b7ed-9d6d0d653842"),
    Preprocess=Input(rid="ri.foundry.main.dataset.98bbe99e-ee9b-4c9a-904a-0a98ad7ce031")
)
def categorical_df(Preprocess):
    df = Preprocess
    parent_cols = [x for x in df.columns if 'parent_' in x]
    df = df.drop(*parent_cols)
    return(df)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c291d8ef-da77-44f3-99a4-ebb2af407881"),
    Preprocess=Input(rid="ri.foundry.main.dataset.98bbe99e-ee9b-4c9a-904a-0a98ad7ce031")
)
def parent_df(Preprocess):
    df = Preprocess
    cat_cols = [x for x in df.columns if 'category_' in x]
    df = df.drop(*cat_cols)
    return(df)
    

