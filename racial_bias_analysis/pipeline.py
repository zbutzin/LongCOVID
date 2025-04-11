from pyspark.sql import functions as F
import pandas as pd

@transform_pandas(
    Output(rid="ri.vector.main.execute.3b761cb8-6462-4b22-b112-eb41e74a95e5"),
    toPandas=Input(rid="ri.foundry.main.dataset.191b7eca-2606-4822-9b56-2d0600b46585")
)
import pandas as pd
def Age(toPandas):
    toPandas['count'] = 1  
    
    bins = [0.0, 17.0, 49.0, 70.0, 107.0]  # Define bin edges
    labels = ["(0.0, 17.0]", "(17.0, 49.0]", "(49.0, 70.0]", "(70.0, 107.0]"]  # Define bin labels
    toPandas['Age Group'] = pd.cut(toPandas['age_at_covid'], bins=bins, labels=labels, right=False, include_lowest=True)
    toPandas['Age Group'] = toPandas['Age Group'].cat.add_categories('Unknown')
    toPandas['Age Group'] = toPandas['Age Group'].fillna('Unknown')
    mypivot = toPandas.pivot_table(index='Age Group', columns='race_ethnicity', values='count', aggfunc='sum', fill_value=0)
    mypivot = mypivot.reset_index()
    mypivotprop = mypivot.copy()
    for label in mypivot.columns:
        if label == 'Age Group':
            continue
        else:
            mypivotprop[label] = mypivotprop[label] / toPandas[toPandas['race_ethnicity'] == label].shape[0] 
    interleaved = pd.DataFrame()
    for i in range(mypivot.shape[1]):
        interleaved[str(i)] = mypivot.iloc[:, i]
        interleaved[str(i + 100)] = mypivotprop.iloc[:, i]
    return interleaved
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.2b703d77-73fa-48bd-b285-ffbe3c72148c"),
    Changed=Input(rid="ri.foundry.main.dataset.d55a0eaf-e5ea-4440-923c-947b8401afbd")
)
import pandas as pd
import numpy as np
def CCI(Changed):
    Final_dataset_with_visits_CCI = Changed.toPandas()

    toPandas = Final_dataset_with_visits_CCI
    toPandas['count'] = 1
    mypivot = toPandas.pivot_table(index='count', columns='race_ethnicity', values='CCI_score_up_through_index_date', aggfunc='mean', fill_value=0)
    mypivotprop = mypivot.copy()
    for label in mypivot.columns:
        if label == 'Age Group':
            continue
        else:
            mypivotprop[label] = np.std(toPandas[toPandas['race_ethnicity'] == label]['CCI_score_up_through_index_date'])
    interleaved = pd.DataFrame()
    for i in range(mypivot.shape[1]):
        interleaved[str(i)] = mypivot.iloc[:, i]
        interleaved[str(i + 100)] = mypivotprop.iloc[:, i]
    return interleaved

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.a661e2f2-a9a1-4d19-a669-3b3d3bcf3220"),
    Changed=Input(rid="ri.foundry.main.dataset.d55a0eaf-e5ea-4440-923c-947b8401afbd")
)
def CCI_Checks(Changed):
    import numpy as np
    toPandas = Changed
    race_list = toPandas['race_ethnicity'].unique()
    race_list.sort()
    for race in race_list:
        myPandas = toPandas[toPandas['race_ethnicity'] == race]
        # check total
        # print(myPandas.shape[0])
        # print(myPandas.shape[0] / toPandas.shape[0])
        # check sex
        # for gender in ['FEMALE', 'MALE']:
        #     myPandas2 = myPandas.copy()
        #     myPandas2 = myPandas2[myPandas2['sex'] == gender]
        #     print(myPandas2.shape[0])
        #     print(myPandas2.shape[0] / myPandas.shape[0]) 
        # check age
        # bins = [0.0, 17.0, 49.0, 70.0, 107.0]  # Define bin edges
        # labels = ["(0.0, 17.0]", "(17.0, 49.0]", "(49.0, 70.0]", "(70.0, 107.0]"]  # Define bin labels
        # myPandas['Age Group'] = pd.cut(myPandas['age_at_covid'], bins=bins, labels=labels, right=False, include_lowest=True)
        # print(myPandas.groupby('Age Group').size())   
        # print(myPandas.groupby('Age Group').size() / myPandas.shape[0])
        # check long covid
        # print(sum(myPandas['long_covid_indicator']))
        # print(sum(myPandas['long_covid_indicator']) / myPandas.shape[0])
        # check monthly visits
        print(np.mean(myPandas['CCI_score_up_through_index_date']))
        print(np.std(myPandas['CCI_score_up_through_index_date']))
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d55a0eaf-e5ea-4440-923c-947b8401afbd"),
    Final_dataset_with_visits_CCI=Input(rid="ri.foundry.main.dataset.27ed7e98-6ae2-4452-9f16-1b37a5146950")
)
def Changed(Final_dataset_with_visits_CCI):
    Final_dataset_with_visits = Final_dataset_with_visits_CCI.toPandas()
    print(Final_dataset_with_visits['race_ethnicity'].unique())
    Final_dataset_with_visits['race_ethnicity'] = Final_dataset_with_visits['race_ethnicity'].replace('Native Hawaiian or Other Pacific Islander Non-Hispanic', 'Asian American or Pacific Islander Non-Hispanic')
    Final_dataset_with_visits['race_ethnicity'] = Final_dataset_with_visits['race_ethnicity'].replace('Asian Non-Hispanic', 'Asian American or Pacific Islander Non-Hispanic')
    return Final_dataset_with_visits

@transform_pandas(
    Output(rid="ri.vector.main.execute.edd26876-b0ab-4b0f-8876-f57f05955eb5"),
    toPandas=Input(rid="ri.foundry.main.dataset.191b7eca-2606-4822-9b56-2d0600b46585")
)
import numpy as np
def Checks(toPandas):
    race_list = toPandas['race_ethnicity'].unique()
    race_list.sort()
    print(race_list)
    for race in race_list:
        myPandas = toPandas[toPandas['race_ethnicity'] == race]
        print(race)
        # check total
        # print(myPandas.shape[0])
        # print(myPandas.shape[0] / toPandas.shape[0])
        # check sex
        # for gender in ['FEMALE', 'MALE']:
        #     myPandas2 = myPandas.copy()
        #     myPandas2 = myPandas2[myPandas2['sex'] == gender]
        #     print(myPandas2.shape[0])
        #     print(myPandas2.shape[0] / myPandas.shape[0]) 
        # check age
        # bins = [0.0, 17.0, 49.0, 70.0, 107.0]  # Define bin edges
        # labels = ["(0.0, 17.0]", "(17.0, 49.0]", "(49.0, 70.0]", "(70.0, 107.0]"]  # Define bin labels
        # myPandas['Age Group'] = pd.cut(myPandas['age_at_covid'], bins=bins, labels=labels, right=False, include_lowest=True)
        # print(myPandas.groupby('Age Group').size())   
        # print(myPandas.groupby('Age Group').size() / myPandas.shape[0])
        # check long covid
        print(sum(myPandas['long_covid_indicator']))
        print(sum(myPandas['long_covid_indicator']) / myPandas.shape[0])
        # check monthly visits
        # print(np.mean(myPandas['pre_visits_per_month']))
        # print(np.std(myPandas['pre_visits_per_month']))
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.d809914a-c577-41d1-9a28-c1f84220d9ec"),
    toPandas=Input(rid="ri.foundry.main.dataset.191b7eca-2606-4822-9b56-2d0600b46585")
)
import pandas as pd
def Long_Covid(toPandas):
    toPandas['count'] = 1
    mypivot = toPandas.pivot_table(index='count', columns='race_ethnicity', values='long_covid_indicator', aggfunc='sum', fill_value=0)
    mypivot = mypivot.reset_index()
    mypivotprop = mypivot.copy()
    for label in mypivot.columns:
        if label == 'count':
            continue
        else:
            mypivotprop[label] = mypivotprop[label] / toPandas[toPandas['race_ethnicity'] == label].shape[0] 
    interleaved = pd.DataFrame()
    for i in range(mypivot.shape[1]):
        interleaved[str(i)] = mypivot.iloc[:, i]
        interleaved[str(i + 100)] = mypivotprop.iloc[:, i]
    return interleaved

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.6ce42714-9e2d-4e0d-b021-f9be0a55e72d"),
    toPandas=Input(rid="ri.foundry.main.dataset.191b7eca-2606-4822-9b56-2d0600b46585")
)
import pandas as pd
import numpy as np
def Monthly_Visits(toPandas):
    toPandas['count'] = 1
    mypivot = toPandas.pivot_table(index='count', columns='race_ethnicity', values='pre_visits_per_month', aggfunc='mean', fill_value=0)
    mypivotprop = mypivot.copy()
    for label in mypivot.columns:
        if label == 'Age Group':
            continue
        else:
            mypivotprop[label] = np.std(toPandas[toPandas['race_ethnicity'] == label]['pre_visits_per_month'])
    interleaved = pd.DataFrame()
    for i in range(mypivot.shape[1]):
        interleaved[str(i)] = mypivot.iloc[:, i]
        interleaved[str(i + 100)] = mypivotprop.iloc[:, i]
    return interleaved
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.49f3709d-3462-45b4-83f4-5e74ae593853"),
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
    Output(rid="ri.vector.main.execute.9114060c-f9c7-4265-8a61-0fc3f0104ce9"),
    toPandas=Input(rid="ri.foundry.main.dataset.191b7eca-2606-4822-9b56-2d0600b46585")
)
def Sex(toPandas):  
    toPandas['sex'] = toPandas['sex'].fillna('Other/Unknown')
    toPandas.loc[(toPandas['sex'] == 'No matching concept') | (toPandas['sex'] == 'OTHER') | (toPandas['sex'] == 'UNKNOWN'), 'sex'] = 'Other/Unknown'
    toPandas['count'] = 1  
    mypivot = toPandas.pivot_table(index='sex', columns='race_ethnicity', values='count', aggfunc='sum', fill_value=0)
    mypivot = mypivot.reset_index()
    mypivotprop = mypivot.copy()
    for label in mypivot.columns:
        if label == 'sex':
            continue
        else:
            mypivotprop[label] = mypivotprop[label] / toPandas[toPandas['race_ethnicity'] == label].shape[0]    
    print(mypivot.columns)
    interleaved = pd.DataFrame()
    for i in range(mypivot.shape[1]):
        interleaved[str(i)] = mypivot.iloc[:, i]
        interleaved[str(i + 100)] = mypivotprop.iloc[:, i]
    return interleaved
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6843263f-7a68-4674-8b2b-e5dee340ef18")
)
from pyspark.sql.types import *
def Table_1():
    schema = StructType([StructField("A", StringType(), True), StructField("B", StringType(), True), StructField("C", StringType(), True), StructField("D", StringType(), True), StructField("E", StringType(), True), StructField("F", StringType(), True), StructField("G", StringType(), True), StructField("H", StringType(), True), StructField("I", StringType(), True), StructField("J", StringType(), True), StructField("K", StringType(), True), StructField("L", StringType(), True), StructField("M", StringType(), True), StructField("N", StringType(), True), StructField("O", StringType(), True), StructField("P", StringType(), True)])
    return spark.createDataFrame([["Characteristic",None,"American Indian or Alaska Native Non-Hispanic Count/Mean","American Indian or Alaska Native Non-Hispanic Percentage/Std Dev","Asian Non-Hispanic Count/Mean","Asian Non-Hispanic Percentage/Std Dev","Black or African American Non-Hispanic Count/Mean","Black or African American Non-Hispanic Percentage/Std Dev","Hispanic or Latino Any Race Count/Mean","Hispanic or Latino Any Race Percentage/Std Dev","Native Hawaiian or Other Pacific Islander Non-Hispanic Count/Mean","Native Hawaiian or Other Pacific Islander Non-Hispanic Percentage/Std Dev","Other Non-Hispanic Count/Mean","Other Non-Hispanic Percentage/Std Dev","White Non-Hispanic Count/Mean","White Non-Hispanic Percentage/Std Dev"],["Total",None,"11546","0.004804415435","106128","0.04416100825","325530","0.1354565526","290302","0.1207978009","9365","0.003896877754","67483","0.02808040592","1592852","0.6628029391"],["Sex","Female","6902","0.5977827819","61047","0.5752204885","196142","0.6025312567","171225","0.5898168115","5203","0.5555792846","35355","0.5239097254","903348","0.567126136"],[None,"Male","4639","0.4017841677","44922","0.4232813207","129285","0.3971523362","118837","0.4093564633","4155","0.4436732515","31544","0.4674362432","688531","0.43226301"],["Age","(0.0, 17.0]","1793","0.155291876","17299","0.1630012815","62087","0.1907258932","77214","0.2659781882","1294","0.1381740523","16593","0.2458841486","191985","0.1205290887"],[None,"(17.0, 49.0]","5064","0.4385934523","47522","0.4477800392","145910","0.448222898","137013","0.4719671239","4174","0.4457020822","32253","0.4779425929","604631","0.3795901942"],[None,"(49.0, 70.0]","3495","0.3027022345","26623","0.2508574551","86935","0.2670567997","57847","0.1992649034","2844","0.3036839295","12905","0.1912333477","488174","0.3064779402"],[None,"(70.0, 107.0]","1194","0.1034124372","14680","0.1383235338","30595","0.09398519338","18227","0.06278633974","1053","0.1124399359","5730","0.0849102737","308051","0.1933958711"],["Medical Conditions","Long Covid","95","0.008227957734","581","0.005474521333","1530","0.004700027647","1270","0.004374754566","56","0.005979711692","162","0.002400604597","10579","0.006641546107"],["Medical Utilization","Medical Visits per Month Prior to COVID-19 Infection","2.378408764","4.773688351","2.076498518","4.418812241","1.978773918","4.432323022","1.881572875","4.346386137","2.551018724","5.410942067","1.025632422","2.629651677","2.274003896","4.807737856"]], schema=schema)

@transform_pandas(
    Output(rid="ri.vector.main.execute.db396934-9aad-4f5e-87a8-dd83f6b0df07"),
    toPandas=Input(rid="ri.foundry.main.dataset.191b7eca-2606-4822-9b56-2d0600b46585")
)
import pandas as pd
def Total(toPandas):
    toPandas['count'] = 1
    toPandas['count2'] = 1
    mypivot = toPandas.pivot_table(index='count', columns='race_ethnicity', values='count2', aggfunc='sum', fill_value=0)
    mypivot = mypivot.reset_index()
    mypivotprop = mypivot.copy()
    for label in mypivot.columns:
        if label == 'count':
            continue
        else:
            mypivotprop[label] = mypivotprop[label] / toPandas.shape[0] 
    interleaved = pd.DataFrame()
    for i in range(mypivot.shape[1]):
        interleaved[str(i)] = mypivot.iloc[:, i]
        interleaved[str(i + 100)] = mypivotprop.iloc[:, i]
    return interleaved
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a72e0c52-de58-454b-b15b-ef4b162df799"),
    AAPI=Input(rid="ri.foundry.main.dataset.3591bbb2-f70e-485b-8359-5f88b85d58a9")
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
def Unadjusted(AAPI):
    df = AAPI
    race_columns = [x for x in df.columns if 'race_ethnicity' in x and x!='race_ethnicity_white_non_hispanic']
    schema = StructType([
        StructField("race", StringType(), True),
        StructField("treatment", IntegerType(), True),
        StructField("n_PASC", DoubleType(), True),
        StructField("Count", IntegerType(), True),
        StructField("Prop", DoubleType(), True)
    ])

    # Create an empty DataFrame with the defined schema
    result = spark.createDataFrame([], schema)
    for c in race_columns:
        df_temp = df.filter((df[c]==1 ) | (df.race_ethnicity_white_non_hispanic == 1)) # either specific race or white
        df_sub = df_temp.groupBy(c).agg(F.sum(F.col('long_covid_indicator')).alias('n_PASC'), F.count(F.col('long_covid_indicator')).alias('Count')).withColumnRenamed(c, 'treatment')
        df_sub = df_sub.withColumn('Proportion', F.col('n_PASC')/F.col('Count'))
        df_sub = df_sub.withColumn('race', F.lit(c)).select(*['race', 'treatment', 'n_PASC', 'Count', 'Proportion'])
        result = result.union(df_sub)
    return result
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0296b71c-7439-42b3-a277-e961d52d79d4"),
    AAPI=Input(rid="ri.foundry.main.dataset.3591bbb2-f70e-485b-8359-5f88b85d58a9")
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

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.191b7eca-2606-4822-9b56-2d0600b46585"),
    Final_dataset_with_visits=Input(rid="ri.foundry.main.dataset.db06f46a-f1ef-4290-9f6d-0cbc176bf2ca")
)
def toPandas(Final_dataset_with_visits):
    Final_dataset_with_visits = Final_dataset_with_visits.toPandas()
    print(Final_dataset_with_visits['race_ethnicity'].unique())
    Final_dataset_with_visits['race_ethnicity'] = Final_dataset_with_visits['race_ethnicity'].replace('Native Hawaiian or Other Pacific Islander Non-Hispanic', 'Asian American or Pacific Islander Non-Hispanic')
    Final_dataset_with_visits['race_ethnicity'] = Final_dataset_with_visits['race_ethnicity'].replace('Asian Non-Hispanic', 'Asian American or Pacific Islander Non-Hispanic')

    return Final_dataset_with_visits
    

