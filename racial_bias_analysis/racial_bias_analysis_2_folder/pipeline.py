from pyspark.sql import functions as F
import pandas as pd

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9ebd355d-f278-42fe-ab27-34582ac316f5"),
    Final_dataset_with_visits_CCI_and_Months_Racial_Bias_=Input(rid="ri.foundry.main.dataset.d32213da-1496-4562-907f-2cd33d834855")
)
from pyspark.sql import SparkSession, Window
def Preprocess(Final_dataset_with_visits_CCI_and_Months_Racial_Bias_):
    df = Final_dataset_with_visits_CCI_and_Months_Racial_Bias_
    # encode sex and race
    s_l = [x[0] for x in df.dtypes if x[1] == 'string']
    s_l.remove('person_id')
    print(s_l)

    for s in s_l:
        df = df.withColumn(s, F.lower(F.regexp_replace(df[s], "[^A-Za-z_0-9]", "_" )))
    df_factor = pd.get_dummies(df.select(s_l).toPandas(), drop_first = False)
    spark = SparkSession.builder.appName("pandas to spark").getOrCreate()
    df_factor = spark.createDataFrame(df_factor)

    w = Window.orderBy(F.lit(1))
    df1 = df.withColumn("rn", F.row_number().over(w)-1)
    df2 = df_factor.withColumn("rn", F.row_number().over(w)-1)
    df = df1.join(df2,["rn"]).drop('rn')

    df = df.drop(*tuple(s_l))
    df = df.drop(*['person_id', 'COVID_first_poslab_or_diagnosis_date', 'long_covid_date'])

    # rename columns
    for col in df.columns:
        if '-' in col:
            new_col = col.replace("-", "_")
            df = df.withColumnRenamed(col, new_col)
    
    df = df.withColumn('cci_ind', F.when(F.col('CCI_score_up_through_index_date').isNull(), 1).otherwise(0))
    # censoring: if post visits >= 6
    df = df.fillna(0)
    df = df.withColumn('post_COVID_visit_indicator', F.when(F.col('visit_count_post')>=6, 1).otherwise(0))

    df = df.drop(*['visit_count_post', 'visit_count_pre', 'pre_covid_length'])
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.21edab8a-a57f-470f-91ab-8ab7972adfae"),
    AAPI=Input(rid="ri.foundry.main.dataset.4eac6916-c300-4c95-a6ac-626fd007f027")
)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySparkToPandas").getOrCreate()

def sum_symptoms(AAPI):
    df = AAPI
    columns_to_sum = [x for x in df.columns if 'sex_' not in x and 'race_' not in x and x not in ['age_at_covid', 'pre_visits_per_month', 'age_at_covid_ind', 'long_covid_indicator', 'post_COVID_visit_indicator', 'CCI_score_up_through_index_date', 'cci_ind']]
    sum_expr = " + ".join(columns_to_sum)

    df = df.withColumn('number_symptoms', F.expr(sum_expr))
    df = df.withColumn('symptom_ind', F.when(F.col('number_symptoms')==0, 0).otherwise(1))
    df = df.withColumn('C1', 1-F.col('post_COVID_visit_indicator'))
    # drop the original symptom columns
    df = df.drop(*columns_to_sum)
    df = df.drop(*['sex_male', 'post_COVID_visit_indicator'])

    # W -> A1 (race) -> C1 (monitoring) —>  —> A2 (symptoms: 0 or not) —> Y (long covid)
    race_columns = [x for x in df.columns if 'race_' in x]
    W_columns = ['age_at_covid', 'pre_visits_per_month', 'age_at_covid_ind', 'sex_female', 'CCI_score_up_through_index_date', 'cci_ind']
    time_columns = ['C1', 'symptom_ind', 'long_covid_indicator']
    # quantiles = df.approxQuantile("number_symptoms", [0.25, 0.5, 0.75], 0.01)

    ordered_columns = W_columns + race_columns + time_columns
    df = df.select(*ordered_columns)

    # # rename C, A, Y
    # new_column_names = ['A1', 'C1', 'A2', 'Y']
    # for old_c, new_c in zip(time_columns, new_column_names):
    #     df = df.withColumnRenamed(old_c, new_c)

    return(df)

