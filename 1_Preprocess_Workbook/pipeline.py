import pandas as pd
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml import Pipeline
from math import *

from datetime import datetime, date, time, timezone, timedelta
import math
from math import ceil

start_date = '2021-10-01'
end_date = '2023-11-15'

followup = 2.5*365

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.871617e4-7817-4dce-b1f3-921d15eb4ec0"),
    PASC_date=Input(rid="ri.vector.main.execute.02726d07-1f70-4531-bd1b-0c8165be9d05"),
    Prediabetes_PCOS_before_COVID=Input(rid="ri.foundry.main.dataset.b5bf6239-7c3e-4b5a-8e3e-b4a7b593bcc4"),
    covariates=Input(rid="ri.foundry.main.dataset.a0be6fa5-8c87-4edd-b45c-ce40929bb3dd"),
    death_date=Input(rid="ri.vector.main.execute.b4ad2735-6a2a-43af-997b-e294e9316769"),
    drug_info=Input(rid="ri.foundry.main.dataset.f6f12941-16c3-4b7c-855b-7f93953642af"),
    last_visit_date=Input(rid="ri.vector.main.execute.f9ed4c51-4a41-4ee4-b447-9603f9f501eb")
)
def all_dates(drug_info, Prediabetes_PCOS_before_COVID, covariates, PASC_date, death_date, last_visit_date):
    df = Prediabetes_PCOS_before_COVID
    # drug_info = drug_info.withColumnRenamed("drug_era_start_date", "condition_start_date")
    df = df.join(drug_info, on=['person_id', 'data_partner_id'], how='inner')
    # diagnosis time <= prescription time
    df = df.filter(df.condition_start_date <= df.drug_era_start_date)

    df = df.join(PASC_date, on=['person_id'], how='left')
    df = df.join(death_date, on=['person_id'], how='left')
    df = df.join(last_visit_date, on=['person_id'], how='left')

    l = list(covariates.select('name').toPandas()['name'])
    l1 = ['person_id', 'drug_type', 'condition_start_date', 'Long_COVID_diagnosis_post_covid_indicator', 'long_covid_date', 'death_date', 'last_visit_date', 'Severity_Type'] + l
    df = df.select(*l1)

    df = df.distinct()

    # Calculate the difference between death_date and condition_start_date
    df = df.withColumn("death_t", F.datediff(col("death_date"), col("condition_start_date")))
    df = df.withColumn("pasc_t", F.datediff(col("long_covid_date"), col("condition_start_date")))
    df = df.withColumn("cens_t", F.datediff(col("last_visit_date"), col("condition_start_date")))

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a0be6fa5-8c87-4edd-b45c-ce40929bb3dd")
)
from pyspark.sql.types import *
def covariates():
    schema = StructType([StructField("name", StringType(), True)])
    return spark.createDataFrame([["BMI_max_observed_or_calculated_before_or_day_of_covid"],["OBESITY_before_or_day_of_covid_indicator"],["SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator"],["DEPRESSION_before_or_day_of_covid_indicator"],["CHRONICLUNGDISEASE_before_or_day_of_covid_indicator"],["DIABETESUNCOMPLICATED_before_or_day_of_covid_indicator"],["HYPERTENSION_before_or_day_of_covid_indicator"],["TOBACCOSMOKER_before_or_day_of_covid_indicator"],["number_of_visits_before_covid"],["age_at_covid"],["race_ethnicity"],["sex"],["cdm_name"],["number_of_COVID_vaccine_doses_before_or_day_of_covid"]], schema=schema)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.77981ce9-bc5f-46dd-b456-484cd5a751af"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
def diagnosis_cohort(concept_set_members):
    # metaformin
    target_concept_names = ['prediabetes', 'Prediabetes-RF', 'prediabetes-mf-analysis', 'N3C-ML-PCOS']
    df = concept_set_members.filter((concept_set_members.concept_set_name.isin(target_concept_names)) & (concept_set_members.is_most_recent_version))

    df = df.withColumn('concept_set_name', 
                       when(col('concept_set_name').isin(['prediabetes', 'Prediabetes-RF', 'prediabetes-mf-analysis']), 'prediabetes')
                       .otherwise('PCOS'))

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f6f12941-16c3-4b7c-855b-7f93953642af"),
    drug_era=Input(rid="ri.foundry.main.dataset.bc6b481a-7b75-470a-addc-66d241e7d7c7"),
    metformin_concepts=Input(rid="ri.foundry.main.dataset.8f1e3e42-76e7-4a06-b565-c69bf5286eaf"),
    ondansetron_concepts=Input(rid="ri.foundry.main.dataset.a41e6892-9e93-4c60-9bb5-ebadfb6f8ac2")
)
def drug_info(drug_era, metformin_concepts, ondansetron_concepts):
    m_concept, o_concept, df = metformin_concepts.select('concept_id'), ondansetron_concepts.select('concept_id'), drug_era
    df_m = df.join(m_concept, df.drug_concept_id == m_concept.concept_id, how='inner').withColumn('drug_type', F.lit('metformin'))
    df_o = df.join(o_concept, df.drug_concept_id == o_concept.concept_id, how='inner').withColumn('drug_type', F.lit('ondansetron'))

    # remove the cases when patient has both medications
    repetitive = df_m.join(df_o, on='person_id', how='inner').select('person_id')
    final = df_m.union(df_o).drop('concept_id').filter((F.col('drug_era_start_date')>=start_date) & (F.col('drug_era_start_date')<=end_date))
    final = final.join(repetitive, on='person_id', how='leftanti')

    return final

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cd30670d-fd3f-455d-a414-93017d7f0816"),
    all_dates=Input(rid="ri.foundry.main.dataset.871617e4-7817-4dce-b1f3-921d15eb4ec0"),
    covariates=Input(rid="ri.foundry.main.dataset.a0be6fa5-8c87-4edd-b45c-ce40929bb3dd"),
    get_first_t=Input(rid="ri.vector.main.execute.523416b3-e35d-4668-aad1-f318f0d5dafc")
)
def events(all_dates, covariates, get_first_t):
    df = get_first_t #can chenge to get_t for all events in multiple rows for each person
    df = df.join(all_dates, on=['person_id'], how='left')

    l = list(covariates.select('name').toPandas()['name'])
    l1 = ['person_id', 'drug_type', 'num_days', 'event', 'Severity_Type'] + l
    df = df.select(*l1)

    df = df.distinct()

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fdfa63b4-500b-4e86-a2a5-2794e8c28c58"),
    events=Input(rid="ri.foundry.main.dataset.cd30670d-fd3f-455d-a414-93017d7f0816")
)
def final(events):
    # filter the person_id in main table
    df = events

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
    # df = df.withColumn("data_partner_id", df["data_partner_id"].cast(StringType()))

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

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5c278e49-8e65-45e8-b40b-779a791fc651"),
    final=Input(rid="ri.foundry.main.dataset.fdfa63b4-500b-4e86-a2a5-2794e8c28c58")
)
def final_ind(final):
    # A column
    df = final.filter(final.event <= 1)
    df = df.withColumn('metformin_ind', when(F.col('drug_type_ondansetron')==1, 0).otherwise(1)).drop('drug_type_ondansetron')

    # Y: 2 years following description
    df = df.withColumn('long_covid_ind', when((F.col('event')==1) & (F.col('num_days')<=followup), 1).otherwise(0))
    df = df.drop('event', 'num_days', 'person_id')

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8f1e3e42-76e7-4a06-b565-c69bf5286eaf"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
# Concept Set Filter (d3a7e119-4daf-493e-984f-97417524d10a): v11
def metformin_concepts(concept_set_members):

    concept_set_name = ""
    # Treat empty string as null
    if len(concept_set_name) == 0:
        concept_set_name = None

    concept_set_id = "140907388"
    # Treat empty string as null
    if len(concept_set_id) == 0:
        concept_set_id = None

    use_most_recent_version = False
    version = ""
    # Treat empty string as null
    if len(version) == 0:
        version = None

    if (concept_set_name and concept_set_id):
        raise ValueError("Only one of 'concept_set_name' or 'concept_set_id' should be filled in")
    elif (concept_set_name is None and concept_set_id is None):
        raise ValueError("Please enter a value for one of 'concept_set_name' or 'concept_set_id'")
    elif concept_set_name:
        df = concept_set_members.filter(concept_set_members.concept_set_name == concept_set_name)
    elif concept_set_id:
        df = concept_set_members.filter(concept_set_members.codeset_id == concept_set_id)
    else:
        raise ValueError("One of concept_set_name or concept_set_id must be filled in")

    if (version is not None and use_most_recent_version):
        raise ValueError("Only one of 'version' or 'use most recent version' should be selected")
    elif (concept_set_id is None and version is None and not use_most_recent_version):
        raise ValueError("Please enter a version or set 'use most recent version' to True")
    if (concept_set_id is not None and (use_most_recent_version or version)):
        raise ValueError("If entering a concept_set_id you should not specify a version or select 'use most recent version'")
    elif version:
        df = df.filter(df.version == int(version))
    elif use_most_recent_version:
        df = df.filter(df.is_most_recent_version == True)

    return df

    

#################################################
## Global imports and functions included below ##
#################################################

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a41e6892-9e93-4c60-9bb5-ebadfb6f8ac2"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
# Concept Set Filter (d3a7e119-4daf-493e-984f-97417524d10a): v11
def ondansetron_concepts(concept_set_members):

    concept_set_name = ""
    # Treat empty string as null
    if len(concept_set_name) == 0:
        concept_set_name = None

    concept_set_id = "800618860"
    # Treat empty string as null
    if len(concept_set_id) == 0:
        concept_set_id = None

    use_most_recent_version = False
    version = ""
    # Treat empty string as null
    if len(version) == 0:
        version = None

    if (concept_set_name and concept_set_id):
        raise ValueError("Only one of 'concept_set_name' or 'concept_set_id' should be filled in")
    elif (concept_set_name is None and concept_set_id is None):
        raise ValueError("Please enter a value for one of 'concept_set_name' or 'concept_set_id'")
    elif concept_set_name:
        df = concept_set_members.filter(concept_set_members.concept_set_name == concept_set_name)
    elif concept_set_id:
        df = concept_set_members.filter(concept_set_members.codeset_id == concept_set_id)
    else:
        raise ValueError("One of concept_set_name or concept_set_id must be filled in")

    if (version is not None and use_most_recent_version):
        raise ValueError("Only one of 'version' or 'use most recent version' should be selected")
    elif (concept_set_id is None and version is None and not use_most_recent_version):
        raise ValueError("Please enter a version or set 'use most recent version' to True")
    if (concept_set_id is not None and (use_most_recent_version or version)):
        raise ValueError("If entering a concept_set_id you should not specify a version or select 'use most recent version'")
    elif version:
        df = df.filter(df.version == int(version))
    elif use_most_recent_version:
        df = df.filter(df.is_most_recent_version == True)

    return df

    

#################################################
## Global imports and functions included below ##
#################################################

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.978f3ea6-f633-420d-9432-bb85442e1a04"),
    events=Input(rid="ri.foundry.main.dataset.cd30670d-fd3f-455d-a414-93017d7f0816")
)
def unnamed_1(events):
    df = events
    df = df.filter((df.event==2) & (df.num_days > 0))
    return df
    

