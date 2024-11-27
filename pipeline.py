

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f2714af8-6f64-4557-a98c-f399eb421167"),
    LL_DO_NOT_DELETE_REQUIRED_concept_sets_all=Input(rid="ri.foundry.main.dataset.284f8923-c023-405c-ac1b-239e99b6d9a2"),
    LL_concept_sets_fusion_everyone=Input(rid="ri.foundry.main.dataset.a6a7765f-9860-4341-9142-c3cbcc58853f")
)
# customized_concept_set_input (98658ea7-2622-4d42-9112-9796c11638ea): v3
#The purpose of this node is to optimize the user's experience connecting a customized concept set "fusion sheet" input data frame to replace LL_concept_sets_fusion_everyone.

def customized_concept_set_input(LL_concept_sets_fusion_everyone, LL_DO_NOT_DELETE_REQUIRED_concept_sets_all):

    required = LL_DO_NOT_DELETE_REQUIRED_concept_sets_all
    customizable = LL_concept_sets_fusion_everyone
    
    df = required.join(customizable, on = required.columns, how = 'outer')
    
    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.662165b8-636e-4d13-b520-cc7ca7c8e606"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
# everyone_cohort (5bd1fe56-a123-4860-aa09-e3b80d103270): v11
#Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for all patients in the N3C enclave. More information can be found in the README linked here (https://unite.nih.gov/workspace/report/ri.report.main.report.855e1f58-bf44-4343-9721-8b4c878154fe).
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/6/23
#Description - This node gathers some commonly used facts about these patients from the "person" and "location" tables, as well as some facts about the patient's institution (from the "manifest" table).  Available age, race, ethnicity, and locations data is gathered at this node.  The patient’s total number of visits as well as the number of days in their observation period is calculated from the “microvisits_to_macrovisits” table in this node after filtering for plausible visit dates based on a site's latest data_extraction_date.  These facts will eventually be joined with the final patient-level table in the final node.

def everyone_cohort(concept_set_members, person, location, manifest, microvisit_to_macrovisit_lds):
        
    """
    Select proportion of enclave patients to use: A value of 1.0 indicates the pipeline will use all patients in the persons table.  
    A value less than 1.0 takes a random sample of the patients with a value of 0.001 (for example) representing a 0.1% sample of the persons table will be used.
    """
    proportion_of_patients_to_use = 1.0

    concepts_df = concept_set_members
    
    person_sample = person \
        .select('person_id','year_of_birth','month_of_birth','day_of_birth','ethnicity_concept_name','race_concept_name','gender_concept_name','location_id','data_partner_id') \
        .withColumnRenamed('gender_concept_name', 'sex') \
        .distinct() \
        .sample(False, proportion_of_patients_to_use, 111)

    manifest_df = manifest \
        .select('data_partner_id','run_date','cdm_name','cdm_version','shift_date_yn','max_num_shift_days') \
        .withColumnRenamed("run_date", "data_extraction_date")
 
    visits_df = microvisit_to_macrovisit_lds.select("person_id", "data_partner_id", "macrovisit_start_date", "visit_start_date") \
        .join(manifest_df.select('data_partner_id', 'data_extraction_date'), on='data_partner_id', how='left') \
        .where(
        (F.col('visit_start_date') >= "2018-01-01") &
        (F.col('visit_start_date') < (F.col('data_extraction_date')+(365*2)))
    ).drop('data_partner_id', 'data_extraction_date')

    location_df = location \
        .dropDuplicates(subset=['location_id']) \
        .select('location_id','city','state','zip','county') \
        .withColumnRenamed('zip','postal_code')   
    
    #join in location_df data to person_sample dataframe 
    df = person_sample.join(location_df, 'location_id', 'left')

    #join in manifest_df information
    df = df.join(manifest_df, 'data_partner_id','inner')
    df = df.withColumn('max_num_shift_days', F.when(F.col('max_num_shift_days')=="", F.lit('0')).otherwise(F.regexp_replace(F.lower('max_num_shift_days'), 'na', '0')))
    
    #calculate date of birth for all patients
    df = df.withColumn("new_year_of_birth", F.when(F.col('year_of_birth').isNull(),1)
                                                .otherwise(F.col('year_of_birth')))
    df = df.withColumn("new_month_of_birth", F.when(F.col('month_of_birth').isNull(), 7)
                                                .when(F.col('month_of_birth')==0, 7)
                                                .otherwise(F.col('month_of_birth')))
    df = df.withColumn("new_day_of_birth", F.when(F.col('day_of_birth').isNull(), 1)
                                                .when(F.col('day_of_birth')==0, 1)
                                                .otherwise(F.col('day_of_birth')))

    df = df.withColumn("date_of_birth", F.concat_ws("-", F.col("new_year_of_birth"), F.col("new_month_of_birth"), F.col("new_day_of_birth")))
    df = df.withColumn("date_of_birth", F.to_date("date_of_birth", format=None)) 

    #convert date of birth string to date and apply min and max reasonable birthdate filter parameters, inclusive
    max_shift_as_int = df.withColumn("shift_days_as_int", F.col('max_num_shift_days').cast(IntegerType())) \
        .select(F.max('shift_days_as_int')) \
        .head()[0]
    min_reasonable_dob = "1902-01-01"
    max_reasonable_dob = F.date_add(F.current_date(), max_shift_as_int)
    df = df.withColumn("date_of_birth", F.when(F.col('date_of_birth').between(min_reasonable_dob, max_reasonable_dob), F.col('date_of_birth')).otherwise(None))

    df = df.withColumn("age", F.floor(F.months_between(max_reasonable_dob, "date_of_birth", roundOff=False)/12))

    H = ['Hispanic']
    A = ['Asian', 'Asian Indian', 'Bangladeshi', 'Bhutanese', 'Burmese', 'Cambodian', 'Chinese', 'Filipino', 'Hmong', 'Indonesian', 'Japanese', 'Korean', 'Laotian', 'Malaysian', 'Maldivian', 'Nepalese', 'Okinawan', 'Pakistani', 'Singaporean', 'Sri Lankan', 'Taiwanese', 'Thai', 'Vietnamese']
    B_AA = ['African', 'African American', 'Barbadian', 'Black', 'Black or African American', 'Dominica Islander', 'Haitian', 'Jamaican', 'Madagascar', 'Trinidadian', 'West Indian']
    W = ['White']
    NH_PI = ['Melanesian', 'Micronesian', 'Native Hawaiian or Other Pacific Islander', 'Other Pacific Islander', 'Polynesian']
    AI_AN = ['American Indian or Alaska Native']
    O = ['More than one race', 'Multiple race', 'Multiple races', 'Other', 'Other Race']
    U = ['Asian or Pacific islander', 'No Information', 'No matching concept', 'Refuse to Answer', 'Unknown', 'Unknown racial group']

    df = df.withColumn("race", F.when(F.col("race_concept_name").isin(H), "Hispanic or Latino")
                        .when(F.col("race_concept_name").isin(A), "Asian")
                        .when(F.col("race_concept_name").isin(B_AA), "Black or African American")
                        .when(F.col("race_concept_name").isin(W), "White")
                        .when(F.col("race_concept_name").isin(NH_PI), "Native Hawaiian or Other Pacific Islander") 
                        .when(F.col("race_concept_name").isin(AI_AN), "American Indian or Alaska Native")
                        .when(F.col("race_concept_name").isin(O), "Other")
                        .when(F.col("race_concept_name").isin(U), "Unknown")
                        .otherwise("Unknown"))

    df = df.withColumn("race_ethnicity", F.when(F.col("ethnicity_concept_name") == 'Hispanic or Latino', "Hispanic or Latino Any Race")
                        .when(F.col("race_concept_name").isin(H), "Hispanic or Latino Any Race")
                        .when(F.col("race_concept_name").isin(A), "Asian Non-Hispanic")
                        .when(F.col("race_concept_name").isin(B_AA), "Black or African American Non-Hispanic")
                        .when(F.col("race_concept_name").isin(W), "White Non-Hispanic")
                        .when(F.col("race_concept_name").isin(NH_PI), "Native Hawaiian or Other Pacific Islander Non-Hispanic") 
                        .when(F.col("race_concept_name").isin(AI_AN), "American Indian or Alaska Native Non-Hispanic")
                        .when(F.col("race_concept_name").isin(O), "Other Non-Hispanic")
                        .when(F.col("race_concept_name").isin(U), "Unknown")
                        .otherwise("Unknown"))

    #create visit counts/obs period dataframes
    hosp_visits = visits_df.where(F.col("macrovisit_start_date").isNotNull()) \
        .orderBy("visit_start_date") \
        .coalesce(1) \
        .dropDuplicates(["person_id", "macrovisit_start_date"]) #hospital
    non_hosp_visits = visits_df.where(F.col("macrovisit_start_date").isNull()) \
        .dropDuplicates(["person_id", "visit_start_date"]) #non-hospital
    visits_df = hosp_visits.union(non_hosp_visits) #join the two

    #total number of visits
    visits_count = visits_df.groupBy("person_id") \
        .count() \
        .select("person_id", F.col('count').alias('total_visits'))

    #obs period in days 
    observation_period = visits_df.groupby('person_id').agg(
        F.max('visit_start_date').alias('pt_max_visit_date'),
        F.min('visit_start_date').alias('pt_min_visit_date')) \
        .withColumn('observation_period', F.datediff('pt_max_visit_date', 'pt_min_visit_date')) \
        .select('person_id', 'observation_period')
    
    #join visit counts/obs periods dataframes with main dataframe
    df = df.join(visits_count, "person_id", "left")
    df = df.join(observation_period, "person_id", "left")

    #LEVEL 2 ONLY
    #df = df.withColumn('max_num_shift_days', F.concat(F.col('max_num_shift_days'), F.lit(" + 180"))).withColumn('shift_date_yn', F.lit('Y'))

    df = df.select('person_id',
        'total_visits',
        'observation_period',
        'sex',
        'city',
        'state',
        'postal_code',
        'county',
        'age',
        'race',
        'race_ethnicity',
        'data_partner_id',
        'data_extraction_date',
        'cdm_name',
        'cdm_version',
        'shift_date_yn',
        'max_num_shift_days')

    return df
    

#################################################
## Global imports and functions included below ##
#################################################
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

@transform_pandas(
    Output(rid="ri.vector.main.execute.3157281f-4709-46bd-918a-df9b0048a7a6")
)
from pyspark.sql.types import *
def unnamed():
    schema = StructType([])
    return spark.createDataFrame([[]], schema=schema)

