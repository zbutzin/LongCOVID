import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan, when, count, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml import Pipeline

from datetime import datetime, date, time, timezone, timedelta
from math import ceil

start_date = date(2021, 12, 25)
# end_date = date(2022, 9, 25)
interval = 60
interval_m = 2
K = 10
L = 4 # fixed collapse length/outcome window

# define outcome window
window_n = [str(i) for i in range(K-L+1, K+1)]

C_outcome, A_outcome, Y_outcome= ['C'+i for i in window_n], ['L'+i+'LTFU' for i in window_n], ['Y'+i for i in window_n]

@transform_pandas(
    Output(rid="ri.vector.main.execute.cb260c79-4ccc-4384-97eb-edaedf19ccf0"),
    C_nodes=Input(rid="ri.foundry.main.dataset.cff588b0-76bc-4f8d-84d7-24dfe29934e2"),
    Cci_index=Input(rid="ri.foundry.main.dataset.8a50ad0e-c074-4fed-8ce0-2d3b99636236"),
    Final_withL=Input(rid="ri.foundry.main.dataset.4d171d5f-1ffb-48d3-813e-cb95e4ccb70f"),
    Preprocessed=Input(rid="ri.foundry.main.dataset.d8070d34-be0f-4109-a394-a6f4e1820f32")
)
def Combined(Final_withL, C_nodes, Preprocessed, Cci_index):
    df = Final_withL.join(Preprocessed, on=['person_id'], how = 'left').join(C_nodes, on = ['person_id', 't0'], how='left').join(Cci_index, on='person_id', how='left')

    null_columns = ['CCI_score_up_through_index_date']
    null_df = df.select(*null_columns)
    for c in null_columns:
        df = df.withColumn('cci' + '_ind', when(col(c).isNull(), 1).otherwise(0))

    # impute the value with mean
    dic = df.select([F.avg(c).alias(c) for c in null_columns]).first().asDict()
    df = df.fillna(dic)
    df = df.withColumnRenamed("CCI_score_up_through_index_date", "cci")
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fef64af0-488b-4480-bb25-f60df6d7314a"),
    type_to_factor=Input(rid="ri.foundry.main.dataset.c2b27c03-6ac0-4a5e-ace0-a51988f97815")
)
def FINAL(type_to_factor):
    # remove columns
    df = type_to_factor
    # df = df.withColumn('num_months_since_base', F.months_between(col('t0'), lit("2021-12-01").cast("date")))

    drop_c = [x for x in df.columns if x[1:2] in window_n or x[1:3] in window_n]
    df = df.drop(*drop_c)
    df = df.drop('vax_to_treatment', 'collapse_l')
    df = df.drop(*[x for x in df.columns if 'vax_type' in x])

    renamed = lambda x: x.replace('_outcome', str(K-L+1))
    for x in ['C_outcome', 'A_outcome', 'Y_outcome']:
        df = df.withColumnRenamed(x, renamed(x))
        
    # order
    LTFU_c, vax_type_c = [x for x in df.columns if 'LTFU' in x], [x for x in df.columns if 'vax_type' in x]
    vax_c = [x for x in df.columns if 'vax' in x and x not in vax_type_c and x[0]=='L' and x[1].isdigit()]

    A_c, C_c, Y_c = [x for x in df.columns if x[0]=='A' and x[1].isdigit()], [x for x in df.columns if x[0]=='C' and x[1].isdigit()],  [x for x in df.columns if x[0]=='Y' and x[1].isdigit()]
    one_c = list(zip(C_c, LTFU_c, vax_c, A_c, Y_c))
    one_c = [i for sub in one_c for i in sub]
    print(one_c)

    # exposure column
    exp_c = [x+str(K-L+1) for x in ['C', 'A', 'Y']]

    all_c = [x for x in df.columns if x not in one_c and x not in exp_c] + one_c + exp_c
    print(all_c)
    df = df.select(*all_c)

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4d0a95d7-137b-4ba9-bc26-0f3bae770c07"),
    Final_with_Censoring_Variables=Input(rid="ri.foundry.main.dataset.3b99f2ab-6bc9-4fa1-8e81-53192f1ea813")
)
def Final_Changed_C_covid(Final_with_Censoring_Variables):
    Final = Final_with_Censoring_Variables
    for i in range(1, 8):
        C_label = 'C_' + str(i)
        Clabel = 'C' + str(i)
        Final[Clabel] = Final[C_label]
        
    for i in range(1, 8):
        C_label = 'C_' + str(i)
        Final.drop(C_label, axis=1, inplace=True)    

    return Final
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3b99f2ab-6bc9-4fa1-8e81-53192f1ea813"),
    FINAL=Input(rid="ri.foundry.main.dataset.fef64af0-488b-4480-bb25-f60df6d7314a")
)
def Final_with_Censoring_Variables(FINAL):
    df = FINAL
    for i in range(1, 6 + 1):
        LTFU_label = 'L' + str(i) + 'LTFU'
        C_label = 'C' + str(i)
        newC_label = 'C_' + str(i)

        df[newC_label] = (~((df[LTFU_label] == 1) & (df[C_label] == 0))).astype(int)
    df['C_7'] = (~((df['A7'] == 1) & (df['C7'] == 0))).astype(int)
    return df
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.2a803c98-8e62-42f6-b485-fecacce92721"),
    filter_patients=Input(rid="ri.vector.main.execute.f6674d91-37e5-4ed1-b4d7-85648d25bed7")
)
# clean (91df5190-5ff4-4ca3-a31e-fd129f465c61): v0
def clean(filter_patients):
    df = filter_patients
    df = df.withColumn('num_months_since_base', F.months_between(col('t0'), lit("2021-12-01").cast("date")))
    drop_c = [x for x in df.columns if 'date' in x or 'vax_to' in x or '_vax_type' in x] + ['t0', 'covid_index', 'long_covid', 'total', 'data_partner_id', 'person_id']
    drop_c.remove('vax_to_treatment')
    df = df.drop(*drop_c)

    # order the covariates, L_nodes, A_nodes, C_nodes, Y_nodes
    LTFU_c, vax_type_c = [x for x in df.columns if 'LTFU' in x], [x for x in df.columns if 'vax_type' in x]
    df = df.fillna(0, LTFU_c)
    df = df.fillna('None', vax_type_c + ['baseline_type'])

    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.528fe54c-8e81-481d-9186-fd3d39de8880"),
    clean=Input(rid="ri.vector.main.execute.2a803c98-8e62-42f6-b485-fecacce92721")
)
def collapse_diff(clean):
    df = clean
    # calculate the required collapse length based on vax_to_treatment
    df = df.withColumn('collapse_l', when((df.vax_to_treatment.isNull()) | (df.vax_to_treatment < 0), K).otherwise(F.ceil((F.col('vax_to_treatment')-1)/interval_m)+K-L))
    
    # make columns outside the range to be NULL
    # loop through outcome columns; if the bin number is larger than collpase_l, make it NULL
    all_names = zip(C_outcome, A_outcome, Y_outcome)
    for c_name, a_name, y_name in all_names:
        cur_i = c_name.split('C')[1]
        df = df.withColumn(c_name, when(F.col('collapse_l') < cur_i, None).otherwise(F.col(c_name)))
        df = df.withColumn(a_name, when(F.col('collapse_l') < cur_i, None).otherwise(F.col(a_name)))
        df = df.withColumn(y_name, when(F.col('collapse_l') < cur_i, None).otherwise(F.col(y_name)))
    
    df = df.withColumn('C_outcome', F.greatest(*C_outcome))
    df = df.withColumn('A_outcome', F.greatest(*A_outcome)) # A_outcome is based on L_LTFU
    df = df.withColumn('Y_outcome', F.greatest(*Y_outcome))

    # if Y_outcome is 1, A_outcome will also be 1
    df = df.withColumn('A_outcome', when(F.col('Y_outcome')==1, 1).otherwise(F.col('A_outcome')))
    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.f6674d91-37e5-4ed1-b4d7-85648d25bed7"),
    Combined=Input(rid="ri.vector.main.execute.cb260c79-4ccc-4384-97eb-edaedf19ccf0")
)
def filter_patients(Combined):
    # if vax_to_treatment is NULL, vax_to_long should also be NULL or negative
    df = Combined
    df = df.filter(df.vax_to_long.isNull() | (df.vax_to_long > 0))
    df = df.filter(df.vax_to_treatment.isNotNull() | (df.vax_to_treatment.isNull() & (df.vax_to_long.isNull())))
    df = df.filter(df.vax_to_treatment.isNull() | (df.vax_to_treatment != 0))

    # fiter vax_to_covid > 1 month and <= 13 months
    df = df.filter(df.vax_to_treatment.isNull() | ((df.vax_to_treatment > 1)))
    # filter covid+1 month before long covid
    df = df.filter(df.vax_to_treatment.isNull() | ((df.vax_to_treatment+1.5) < df.vax_to_long) | df.vax_to_long.isNull())

    # filter at least 2 visits before covid
    # df = df.filter(df.number_of_visits_before_covid >= 2)

    # filter who got long covid within 20 months
    # df = df.filter((df.vax_to_long <= 21) | df.vax_to_long.isNull())

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c2b27c03-6ac0-4a5e-ace0-a51988f97815"),
    collapse_diff=Input(rid="ri.vector.main.execute.528fe54c-8e81-481d-9186-fd3d39de8880")
)
# type_to_factor (47455002-501f-4452-a995-bc782abb18c3): v1
def type_to_factor(collapse_diff):
    df = collapse_diff
    target_c = 'baseline_type'
    df = df.withColumn(target_c, F.lower(F.regexp_replace(df[target_c], "[^A-Za-z_0-9]", "_" )))
    df_factor = pd.get_dummies(df.select(target_c).toPandas(), drop_first = True)
    spark = SparkSession.builder.appName("pandas to spark").getOrCreate()
    df_factor = spark.createDataFrame(df_factor)
    df = df.drop(target_c)

    # join
    w = Window.orderBy(F.lit(1))
    df = df.withColumn("rn", F.row_number().over(w)-1)
    df_factor = df_factor.withColumn("rn", F.row_number().over(w)-1)

    df = df.join(df_factor,["rn"]).drop('rn')

    return df

    

