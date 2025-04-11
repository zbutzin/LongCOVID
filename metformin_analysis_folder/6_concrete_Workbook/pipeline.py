from pyspark.sql.functions import col, when, avg, lit, row_number, lower, regexp_replace
from pyspark.sql.types import IntegerType, StringType
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f8e0ad06-7858-42a1-99d3-6b4bf3ee1067"),
    Final_table_3_5=Input(rid="ri.foundry.main.dataset.423c986f-e0e8-4e4e-b5a0-6109fd9175f1")
)
def preprocess(Final_table_3_5):
    df = Final_table_3_5
    # filter on inclusion dates
    df = df.filter((col("date") >= '2021-10-01') & (col("date") <= '2024-01-01'))
    # filter on PCOS or PREDIABETES
    df = df.filter((col("PCOS_indicator") == 1) | (col("PREDIABETESRF_indicator") == 1))

    # drop unnecessary columns
    drop_columns = [x for x in df.columns if 'date' in x] + ['n3c_metformin_before_180', 'n3c_metformin_before_365', 'no_other_medications', 'drug_concept_name', 'sex', 'race_ethnicity', 'new_person_id', 'drug_concept_id', 'age_at_covid', 'levothyroxine', 'ondansetron']
    print(drop_columns)
    df = df.drop(*drop_columns)
    
    # impute null columns
    null_df = df.select([c for c in df.columns if df.filter(col(c).isNull()).count() > 0])
    print(null_df.columns) 
    ind_df = df.select([when(col(c).isNull(), 1).otherwise(0).alias(c+'_ind') for c in null_df.columns])

    # impute the value with mean
    dic = df.select([avg(c).alias(c) for c in null_df.columns]).first().asDict()
    df = df.fillna(dic)

    # encode one-hot encode data partner id
    df = df.withColumn("data_partner_id", df["data_partner_id"].cast(StringType()))
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

