

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.da40e627-9c72-416b-8cb6-8d13d6595dee"),
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
    Output(rid="ri.foundry.main.dataset.07b3342a-21cb-4465-a91b-bf634abe3746"),
    LL_DO_NOT_DELETE_REQUIRED_concept_sets_all=Input(rid="ri.foundry.main.dataset.284f8923-c023-405c-ac1b-239e99b6d9a2"),
    LL_concept_sets_fusion_everyone=Input(rid="ri.foundry.main.dataset.a6a7765f-9860-4341-9142-c3cbcc58853f")
)
# customized_concept_set_input (98658ea7-2622-4d42-9112-9796c11638ea): v3
#The purpose of this node is to optimize the user's experience connecting a customized concept set "fusion sheet" input data frame to replace LL_concept_sets_fusion_everyone.

def customized_concept_set_input_1(LL_concept_sets_fusion_everyone, LL_DO_NOT_DELETE_REQUIRED_concept_sets_all):

    required = LL_DO_NOT_DELETE_REQUIRED_concept_sets_all
    customizable = LL_concept_sets_fusion_everyone
    
    df = required.join(customizable, on = required.columns, how = 'outer')
    
    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d5cd793d-2c52-4610-afc2-b599566561aa"),
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
    Output(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94"),
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

def everyone_cohort_1(concept_set_members, person, location, manifest, microvisit_to_macrovisit_lds):
        
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
    Output(rid="ri.foundry.main.dataset.9ff8d881-d252-4e93-81c3-dae7a4800f14"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    customized_concept_set_input_1=Input(rid="ri.foundry.main.dataset.07b3342a-21cb-4465-a91b-bf634abe3746"),
    everyone_cohort_1=Input(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94")
)
# everyone_conditions_of_interest (aa2c2552-ecc8-48ec-a5d5-782f2fdea16c): v3
#Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for all patients in the N3C enclave.
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/7/22
#Description - This node filters the condition_eras table for rows that have a condition_concept_id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these conditions are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.

def everyone_conditions_of_interest(everyone_cohort_1, concept_set_members, condition_occurrence, customized_concept_set_input_1):

    #bring in only cohort patient ids
    persons = everyone_cohort_1.select('person_id')
    #filter observations table to only cohort patients    
    conditions_df = condition_occurrence \
        .select('person_id', 'condition_start_date', 'condition_concept_id') \
        .where(F.col('condition_start_date').isNotNull()) \
        .withColumnRenamed('condition_start_date','date') \
        .withColumnRenamed('condition_concept_id','concept_id') \
        .join(persons,'person_id','inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the conditions domain
    fusion_df = customized_concept_set_input_1 \
        .filter(customized_concept_set_input_1.domain.contains('condition')) \
        .select('concept_set_name','indicator_prefix')
    #filter concept set members table to only concept ids for the conditions of interest
    concepts_df = concept_set_members \
        .select('concept_set_name', 'is_most_recent_version', 'concept_id') \
        .where(F.col('is_most_recent_version')=='true') \
        .join(fusion_df, 'concept_set_name', 'inner') \
        .select('concept_id','indicator_prefix')

    #find conditions information based on matching concept ids for conditions of interest
    df = conditions_df.join(concepts_df, 'concept_id', 'inner')
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for conditions of interest    
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(F.lit(1)).na.fill(0)
   
    return df
    

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a543ae37-725d-4f59-af9e-b182db43f9bf"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    customized_concept_set_input_1=Input(rid="ri.foundry.main.dataset.07b3342a-21cb-4465-a91b-bf634abe3746"),
    device_exposure=Input(rid="ri.foundry.main.dataset.d685db48-6583-43d6-8dc5-a9ebae1a827a"),
    everyone_cohort_1=Input(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94")
)
# everyone_devices_of_interest (4d1a4b5f-2b99-4ca7-b476-dafc4f16ea37): v4
#Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for all patients in the N3C enclave.
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/7/22
#Description - This nodes filter the source OMOP tables for rows that have a standard concept id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these variables are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.

def everyone_devices_of_interest(device_exposure, everyone_cohort_1, concept_set_members, customized_concept_set_input_1):

    #bring in only cohort patient ids
    persons = everyone_cohort_1.select('person_id')
    #filter device exposure table to only cohort patients
    devices_df = device_exposure \
        .select('person_id','device_exposure_start_date','device_concept_id') \
        .where(F.col('device_exposure_start_date').isNotNull()) \
        .withColumnRenamed('device_exposure_start_date','date') \
        .withColumnRenamed('device_concept_id','concept_id') \
        .join(persons,'person_id','inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the devices domain
    fusion_df = customized_concept_set_input_1 \
        .filter(customized_concept_set_input_1.domain.contains('device')) \
        .select('concept_set_name','indicator_prefix')
    #filter concept set members table to only concept ids for the devices of interest
    concepts_df = concept_set_members \
        .select('concept_set_name', 'is_most_recent_version', 'concept_id') \
        .where(F.col('is_most_recent_version')=='true') \
        .join(fusion_df, 'concept_set_name', 'inner') \
        .select('concept_id','indicator_prefix')
        
    #find device exposure information based on matching concept ids for devices of interest
    df = devices_df.join(concepts_df, 'concept_id', 'inner')
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for devices of interest
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(F.lit(1)).na.fill(0)

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.390a704a-fffa-48f5-8966-dd6a59a5fd8a"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    customized_concept_set_input_1=Input(rid="ri.foundry.main.dataset.07b3342a-21cb-4465-a91b-bf634abe3746"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef"),
    everyone_cohort_1=Input(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94")
)
# everyone_drugs_of_interest (ef43cf2e-5442-4ad8-924b-27e1d7ca17f7): v4
#Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for all patients in the N3C enclave.
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/7/22
#Description - This nodes filter the source OMOP tables for rows that have a standard concept id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these variables are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.

def everyone_drugs_of_interest(concept_set_members, drug_exposure, everyone_cohort_1, customized_concept_set_input_1):
  
    #bring in only cohort patient ids
    persons = everyone_cohort_1.select('person_id')
    #filter drug exposure table to only cohort patients    
    drug_df = drug_exposure \
        .select('person_id','drug_exposure_start_date','drug_concept_id') \
        .where(F.col('drug_exposure_start_date').isNotNull()) \
        .withColumnRenamed('drug_exposure_start_date','date') \
        .withColumnRenamed('drug_concept_id','concept_id') \
        .join(persons,'person_id','inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the drug domain
    fusion_df = customized_concept_set_input_1 \
        .filter(customized_concept_set_input_1.domain.contains('drug')) \
        .select('concept_set_name','indicator_prefix')
    #filter concept set members table to only concept ids for the drugs of interest
    concepts_df = concept_set_members \
        .select('concept_set_name', 'is_most_recent_version', 'concept_id') \
        .where(F.col('is_most_recent_version')=='true') \
        .join(fusion_df, 'concept_set_name', 'inner') \
        .select('concept_id','indicator_prefix')
        
    #find drug exposure information based on matching concept ids for drugs of interest
    df = drug_df.join(concepts_df, 'concept_id', 'inner')
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for drugs of interest
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(F.lit(1)).na.fill(0)

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ce5657f-1ce5-488f-93f7-066a842f994c"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    everyone_cohort_1=Input(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
# everyone_measurements_of_interest (8a94c2ef-d733-4e49-a2ed-05cd9c040fb3): v6
#Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for all patients in the N3C enclave.
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/7/22
#Description - This node filters the measurements table for rows that have a measurement_concept_id associated with one of the concept sets described in the data dictionary in the README.  Indicator names for a positive COVID PCR or AG test, negative COVID PCR or AG test, positive COVID antibody test, and negative COVID antibody test are assigned, and the indicators are collapsed to unique instances on the basis of patient and date. It also finds the harmonized value as a number for BMI measurements and collapses these values to unique instances on the basis of patient and date.  Measurement BMI cutoffs included are intended for adults. Analyses focused on pediatric measurements should use different bounds for BMI measurements. 

def everyone_measurements_of_interest(measurement, concept_set_members, everyone_cohort_1):
    
    #bring in only cohort patient ids
    persons = everyone_cohort_1.select('person_id')
    #filter procedure occurrence table to only cohort patients    
    df = measurement \
        .select('person_id','measurement_date','measurement_concept_id','harmonized_value_as_number','value_as_concept_id') \
        .where(F.col('measurement_date').isNotNull()) \
        .withColumnRenamed('measurement_date','date') \
        .join(persons,'person_id','inner')
        
    concepts_df = concept_set_members \
        .select('concept_set_name', 'is_most_recent_version', 'concept_id') \
        .where(F.col('is_most_recent_version')=='true')
          
    #Find BMI closest to COVID using both reported/observed BMI and calculated BMI using height and weight.  Cutoffs for reasonable height, weight, and BMI are provided and can be changed by the template user.
    lowest_acceptable_BMI = 10
    highest_acceptable_BMI = 100
    lowest_acceptable_weight = 5 #in kgs
    highest_acceptable_weight = 300 #in kgs
    lowest_acceptable_height = .6 #in meters
    highest_acceptable_height = 2.43 #in meters

    bmi_codeset_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="body mass index") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
    weight_codeset_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="Body weight (LG34372-9 and SNOMED)") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
    height_codeset_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="Height (LG34373-7 + SNOMED)") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
    
    pcr_ag_test_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="ATLAS SARS-CoV-2 rt-PCR and AG") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
    antibody_test_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="Atlas #818 [N3C] CovidAntibody retry") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
    covid_positive_measurement_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="ResultPos") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
    covid_negative_measurement_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="ResultNeg") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])

 
    #add value columns for rows associated with the above concept sets, but only include BMI or height or weight when in reasonable range
    BMI_df = df.where(F.col('harmonized_value_as_number').isNotNull()) \
        .withColumn('Recorded_BMI', F.when(df.measurement_concept_id.isin(bmi_codeset_ids) & df.harmonized_value_as_number.between(lowest_acceptable_BMI, highest_acceptable_BMI), df.harmonized_value_as_number).otherwise(0)) \
        .withColumn('height', F.when(df.measurement_concept_id.isin(height_codeset_ids) & df.harmonized_value_as_number.between(lowest_acceptable_height, highest_acceptable_height), df.harmonized_value_as_number).otherwise(0)) \
        .withColumn('weight', F.when(df.measurement_concept_id.isin(weight_codeset_ids) & df.harmonized_value_as_number.between(lowest_acceptable_weight, highest_acceptable_weight), df.harmonized_value_as_number).otherwise(0)) 
        
    labs_df = df.withColumn('PCR_AG_Pos', F.when(df.measurement_concept_id.isin(pcr_ag_test_ids) & df.value_as_concept_id.isin(covid_positive_measurement_ids), 1).otherwise(0)) \
        .withColumn('PCR_AG_Neg', F.when(df.measurement_concept_id.isin(pcr_ag_test_ids) & df.value_as_concept_id.isin(covid_negative_measurement_ids), 1).otherwise(0)) \
        .withColumn('Antibody_Pos', F.when(df.measurement_concept_id.isin(antibody_test_ids) & df.value_as_concept_id.isin(covid_positive_measurement_ids), 1).otherwise(0)) \
        .withColumn('Antibody_Neg', F.when(df.measurement_concept_id.isin(antibody_test_ids) & df.value_as_concept_id.isin(covid_negative_measurement_ids), 1).otherwise(0))
     
    #collapse all reasonable values to unique person and visit rows
    BMI_df = BMI_df.groupby('person_id', 'date').agg(
    F.max('Recorded_BMI').alias('Recorded_BMI'),
    F.max('height').alias('height'),
    F.max('weight').alias('weight'))
    labs_df = labs_df.groupby('person_id', 'date').agg(
    F.max('PCR_AG_Pos').alias('PCR_AG_Pos'),
    F.max('PCR_AG_Neg').alias('PCR_AG_Neg'),
    F.max('Antibody_Pos').alias('Antibody_Pos'),
    F.max('Antibody_Neg').alias('Antibody_Neg'))

    #add a calculated BMI for each visit date when height and weight available.  Note that if only one is available, it will result in zero
    #subsequent filter out rows that would have resulted from unreasonable calculated_BMI being used as best_BMI for the visit 
    BMI_df = BMI_df.withColumn('calculated_BMI', (BMI_df.weight/(BMI_df.height*BMI_df.height)))
    BMI_df = BMI_df.withColumn('BMI', F.when(BMI_df.Recorded_BMI>0, BMI_df.Recorded_BMI).otherwise(BMI_df.calculated_BMI)) \
        .select('person_id','date','BMI')
    BMI_df = BMI_df.filter((BMI_df.BMI<=highest_acceptable_BMI) & (BMI_df.BMI>=lowest_acceptable_BMI)) \
        .withColumn('BMI_rounded', F.round(BMI_df.BMI)) \
        .drop('BMI')
    BMI_df = BMI_df.withColumn('OBESITY', F.when(BMI_df.BMI_rounded>=30, 1).otherwise(0))

    #join BMI_df with labs_df to retain all lab results with only reasonable BMI_rounded and OBESITY flags
    df = labs_df.join(BMI_df, on=['person_id', 'date'], how='left')

    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.430cce14-9e74-4677-8e86-2d1acc181ac8"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    customized_concept_set_input_1=Input(rid="ri.foundry.main.dataset.07b3342a-21cb-4465-a91b-bf634abe3746"),
    everyone_cohort_1=Input(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94"),
    observation=Input(rid="ri.foundry.main.dataset.b998b475-b229-471c-800e-9421491409f3")
)
# everyone_observations_of_interest (e53256c5-3a8b-4616-a4a5-9501076a6c1a): v5
#Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for all patients in the N3C enclave.
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/7/22
#Description - This nodes filter the source OMOP tables for rows that have a standard concept id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these variables are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.

def everyone_observations_of_interest(observation, concept_set_members, everyone_cohort_1, customized_concept_set_input_1):
   
    #bring in only cohort patient ids
    persons = everyone_cohort_1.select('person_id')
    #filter observations table to only cohort patients    
    observations_df = observation \
        .select('person_id','observation_date','observation_concept_id') \
        .where(F.col('observation_date').isNotNull()) \
        .withColumnRenamed('observation_date','date') \
        .withColumnRenamed('observation_concept_id','concept_id') \
        .join(persons,'person_id','inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the observations domain
    fusion_df = customized_concept_set_input_1 \
        .filter(customized_concept_set_input_1.domain.contains('observation')) \
        .select('concept_set_name','indicator_prefix')
    #filter concept set members table to only concept ids for the observations of interest
    concepts_df = concept_set_members \
        .select('concept_set_name', 'is_most_recent_version', 'concept_id') \
        .where(F.col('is_most_recent_version')=='true') \
        .join(fusion_df, 'concept_set_name', 'inner') \
        .select('concept_id','indicator_prefix')

    #find observations information based on matching concept ids for observations of interest
    df = observations_df.join(concepts_df, 'concept_id', 'inner')
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for observations of interest    
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(F.lit(1)).na.fill(0)

    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6a5033ad-82ec-4cbf-8246-3c559ea36b59"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    death=Input(rid="ri.foundry.main.dataset.d8cc2ad4-215e-4b5d-bc80-80ffb3454875"),
    everyone_cohort_1=Input(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
# everyone_patient_deaths (579dce2c-ecb4-4cb9-bbbe-16be6fd15c88): v6
#Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for all patients in the N3C enclave.
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/6/23
#Description - This node filters the visits table for rows that have a discharge_to_concept_id that corresponds with the DECEASED or HOSPICE concept sets and combines these records with the patients in the deaths table. Death dates are taken from the deaths table and from the visits table if the patient has a discharge_to_concept_id that corresponds with the DECEASED concept set. Death dates are prioritized such that we filter to only plausible death dates from the OMOP death table and take a min prior to adding in any additional plausible death dates from the OMOP visits table with concept id belonging to the DECEASED concept set and take a max for patients who do not already have a death date from the OMOP death table. No date is retained for patients who were discharged to hospice.

def everyone_patient_deaths(death, microvisit_to_macrovisit_lds, concept_set_members, everyone_cohort_1):
 
    persons = everyone_cohort_1.select('person_id', 'data_extraction_date')
    death_df = death \
        .select('person_id', 'death_date') \
        .distinct() \
        .join(persons, on = 'person_id', how = 'inner')
    visits_df = microvisit_to_macrovisit_lds \
        .select('person_id', 'visit_start_date', 'visit_end_date', 'discharge_to_concept_id') \
        .distinct() \
        .join(persons, on = 'person_id', how = 'inner')
    concepts_df = concept_set_members \
        .select('concept_set_name', 'is_most_recent_version', 'concept_id') \
        .where(F.col('is_most_recent_version')=='true')

    #create lists of concept ids to look for in the discharge_to_concept_id column of the visits_df
    death_from_visits_ids = list(concepts_df.where(F.col('concept_set_name') == "DECEASED").select('concept_id').toPandas()['concept_id'])
    hospice_from_visits_ids = list(concepts_df.where(F.col('concept_set_name') == "HOSPICE").select('concept_id').toPandas()['concept_id'])

    #filter visits table to patient and date rows that have DECEASED that matches list of concept_ids
    death_from_visits_df = visits_df \
        .where(F.col('discharge_to_concept_id').isin(death_from_visits_ids)) \
        .drop('discharge_to_concept_id') \
        .distinct()
    #filter visits table to patient rows that have HOSPICE that matches list of concept_ids
    hospice_from_visits_df = visits_df.drop('visit_start_date', 'visit_end_date', 'data_extraction_date') \
        .where(F.col('discharge_to_concept_id').isin(hospice_from_visits_ids)) \
        .drop('discharge_to_concept_id') \
        .distinct()

    #####################################################################
    ###combine relevant visits sourced deaths with deaths table deaths###
    #####################################################################
   
    #keep rows where death_date is plausible 
    #then take earliest recorded date per person
    death_w_dates_df = death_df.where(
        (F.col('death_date') >= "2018-01-01") &
        (F.col('death_date') < (F.col('data_extraction_date')+(365*2)))
    ).groupby('person_id').agg(F.min('death_date').alias('death_date')) \
    .drop('data_extraction_date')

    #from rows of visit table that have concept_id belonging to DECEASED concept set,
    #create new column visit_death_date that is plauisble visit_end_date when available,
    #and plausible visit_start_date when visit_end_date is Null or not plausible
    #then take latest recorded date per person
    death_from_visits_w_dates_df = death_from_visits_df \
        .withColumn('visit_death_date', F.when(
            (F.col('visit_end_date') >= "2018-01-01") &
            (F.col('visit_end_date') < (F.col('data_extraction_date')+(365*2))), F.col('visit_end_date')
            ).when(
            (F.col('visit_start_date') >= "2018-01-01") &
            (F.col('visit_start_date') < (F.col('data_extraction_date')+(365*2))), F.col('visit_start_date')
            ).otherwise(None)
        ).groupby('person_id').agg(F.max('visit_death_date').alias('visit_death_date')) \
        .drop('data_extraction_date')

    #join deaths with dates from both domains
    df = death_w_dates_df.join(death_from_visits_w_dates_df, on = 'person_id', how = 'outer')
    #prioritize death_dates from the deaths table over from visits table
    df = df.withColumn('date', F.when(F.col('death_date').isNotNull(), F.col('death_date')).otherwise(F.col('visit_death_date'))) \
        .drop('death_date', 'visit_death_date')

    #join in patients, without any date, from deaths table
    #join in patients, without any date, from visits table
    #join in patients, without any date, for HOSPICE from visits table
    #inner join with cohort node patients to keep only confirmed covid patients
    df = df.join(death_df.select('person_id'), on='person_id', how='outer') \
        .join(death_from_visits_df.select('person_id'), on='person_id', how='outer') \
        .join(hospice_from_visits_df, on='person_id', how='outer') \
        .join(persons, on = 'person_id', how = 'inner') \
        .dropDuplicates()
    #flag all patients as having died regardless of date
    df = df.withColumn("patient_death", F.lit(1))
    
    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.17608ec7-985a-40fa-9307-7b27a5ec60cc"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    customized_concept_set_input_1=Input(rid="ri.foundry.main.dataset.07b3342a-21cb-4465-a91b-bf634abe3746"),
    everyone_cohort_1=Input(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94"),
    procedure_occurrence=Input(rid="ri.foundry.main.dataset.f6f0b5e0-a105-403a-a98f-0ee1c78137dc")
)
# everyone_procedures_of_interest (abff519b-bbe7-409e-b4da-86147fad95ab): v4
#Purpose - The purpose of this pipeline is to produce a day level and a persons level fact table for all patients in the N3C enclave.
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/7/22
#Description - This nodes filter the source OMOP tables for rows that have a standard concept id associated with one of the concept sets described in the data dictionary in the README through the use of a fusion sheet.  Indicator names for these variables are assigned, and the indicators are collapsed to unique instances on the basis of patient and date.

def everyone_procedures_of_interest(everyone_cohort_1, concept_set_members, procedure_occurrence, customized_concept_set_input_1):
  
    #bring in only cohort patient ids
    persons = everyone_cohort_1.select('person_id')
    #filter procedure occurrence table to only cohort patients    
    procedures_df = procedure_occurrence \
        .select('person_id','procedure_date','procedure_concept_id') \
        .where(F.col('procedure_date').isNotNull()) \
        .withColumnRenamed('procedure_date','date') \
        .withColumnRenamed('procedure_concept_id','concept_id') \
        .join(persons,'person_id','inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the procedure domain
    fusion_df = customized_concept_set_input_1 \
        .filter(customized_concept_set_input_1.domain.contains('procedure')) \
        .select('concept_set_name','indicator_prefix')
    #filter concept set members table to only concept ids for the procedures of interest
    concepts_df = concept_set_members \
        .select('concept_set_name', 'is_most_recent_version', 'concept_id') \
        .where(F.col('is_most_recent_version')=='true') \
        .join(fusion_df, 'concept_set_name', 'inner') \
        .select('concept_id','indicator_prefix')
 
    #find procedure occurrence information based on matching concept ids for procedures of interest
    df = procedures_df.join(concepts_df, 'concept_id', 'inner')
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for procedures of interest    
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(F.lit(1)).na.fill(0)

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8e3f2c95-dc02-4853-ad64-1a31fd8411ec"),
    Vaccine_fact_lds=Input(rid="ri.foundry.main.dataset.7482e426-55a2-4a0b-9976-4cb3aa35788d"),
    everyone_cohort_1=Input(rid="ri.foundry.main.dataset.292e6cad-ead9-45db-a9bf-ed7bdad78c94")
)
# everyone_vaccines_of_interest (6ebcb216-3b16-4d7b-ad3e-bdde57ed0a24): v1
#Purpose - The purpose of this pipeline is to produce a visit day level and a persons level fact table for all patients in the N3C enclave.
#Creator/Owner/contact - Andrea Zhou
#Last Update - 12/7/22
#Description - This node converts the vaccine_fact dataset from columns of dates for each dose number to a simple flag for dates on which a vaccine dose was received by the patient. The indicator is collapsed to unique instances on the basis of patient and date.

def everyone_vaccines_of_interest(Vaccine_fact_lds, everyone_cohort_1):
    
    persons = everyone_cohort_1.select('person_id')
    vax_df = Vaccine_fact_lds.select('person_id', '1_vax_date', '2_vax_date', '3_vax_date', '4_vax_date') \
        .join(persons, 'person_id', 'inner')

    first_dose = vax_df.select('person_id', '1_vax_date') \
        .withColumnRenamed('1_vax_date', 'date') \
        .where(F.col('date').isNotNull())
    second_dose = vax_df.select('person_id', '2_vax_date') \
        .withColumnRenamed('2_vax_date', 'date') \
        .where(F.col('date').isNotNull())        
    third_dose = vax_df.select('person_id', '3_vax_date') \
        .withColumnRenamed('3_vax_date', 'date') \
        .where(F.col('date').isNotNull())
    fourth_dose = vax_df.select('person_id', '4_vax_date') \
        .withColumnRenamed('4_vax_date', 'date') \
        .where(F.col('date').isNotNull())

    df = first_dose.join(second_dose, on=['person_id', 'date'], how='outer') \
        .join(third_dose, on=['person_id', 'date'], how='outer') \
        .join(fourth_dose, on=['person_id', 'date'], how='outer') \
        .distinct()

    df = df.withColumn('had_vaccine_administered', F.lit(1))

    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.vector.main.execute.9b0d833d-bb7b-40e1-a629-6248804b664a")
)
from pyspark.sql.types import *
def unnamed():
    schema = StructType([])
    return spark.createDataFrame([[]], schema=schema)

