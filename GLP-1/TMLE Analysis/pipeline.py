from pyspark.sql.functions import col, when, avg, lit, row_number, lower, regexp_replace, median
from pyspark.sql.types import IntegerType, StringType
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5915f972-df80-4a61-a80a-64abca276e4c")
)
def Final(death, Final_table_4):
    df = (Final_table_4.alias("f")\
                .join(death.alias('d'), Final_table_4.new_person_id == death.person_id, how='left')
                .select("f.*", col("d.death_date").alias("death_date")))

    return(df)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f4b43eb2-1875-448f-8e33-a269cd18616f"),
    select_columns=Input(rid="ri.foundry.main.dataset.055f9d2e-169f-4a9e-9cfd-dbc9bbdf0629")
)
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array

def Preprocess(select_columns):
    """
    • Adds a *_ind column for every column that contains nulls
      (1 ⇢ value is missing, 0 ⇢ value present).

    • One‑hot‑encodes `data_partner_id` with Spark ML.
      The resulting column is a sparse vector named `data_partner_id_ohe`.
      (The intermediate index column is dropped.)
    """
    
    df = select_columns

    # ── 1. indicator columns for nulls ───────────────────────────────────────
    cols_with_nulls = [
        c for c in df.columns
        if df.where(F.col(c).isNull()).limit(1).count()      # fast early‑exit
    ]

    for c in cols_with_nulls:
        df = df.withColumn(f"{c}_ind", F.when(F.col(c).isNull(), 1).otherwise(0))

    # ── 2. one‑hot‑encode data_partner_id ────────────────────────────────────
    # make sure the column is a string
    indexer = StringIndexer(
        inputCol="data_partner_id",
        outputCol="data_partner_id_idx",
        handleInvalid="keep")

    encoder = OneHotEncoder(
        inputCol="data_partner_id_idx",
        outputCol="data_partner_id_ohe",
        dropLast=False)

    pipe_model = Pipeline(stages=[indexer, encoder]).fit(df)
    df         = pipe_model.transform(df)

    # ── 3. Explode the vector into separate dummy columns ─────────────────
    # 3‑a. The StringIndexer labels tell us which numeric index ↔ which ID
    idx_to_id = list(map(int, pipe_model.stages[0].labels))   # e.g. [698, 726, 124, …]

    # 3‑b. Convert VectorUDT → ArrayType[Double] once
    df = df.withColumn("_dparr", vector_to_array("data_partner_id_ohe"))

    # 3‑c. For each index, add a column like data_partner_id_698, cast to int
    for i, partner_id in enumerate(idx_to_id):
        df = df.withColumn(f"data_partner_id_{partner_id}",
                           F.col("_dparr")[i].cast("int"))

    # ── 4. House‑cleaning ─────────────────────────────────────────────────
    df = (df
          .drop("data_partner_id_idx", "data_partner_id_ohe", "_dparr", 'data_partner_id')  # keep things tidy
          .na.fill(0)) 
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.055f9d2e-169f-4a9e-9cfd-dbc9bbdf0629"),
    Final_table_11=Input(rid="ri.foundry.main.dataset.71bd0ed7-7b19-4b3c-94e4-b38f91de9269")
)
def select_columns(Final_table_11):
    Final_table_4 = Final_table_11
    from pyspark.sql import functions as F

    # 1) grab prefixes -----------------------------------------------------------
    prefix_cols = [
        c for c in Final_table_4.columns
        if (c.startswith("sex_"))                                       # all sex_…
        or (c.startswith("race_") and "ethnicity" not in c)          # race_ but not race_ethnicity…
    ]

    # 2) add the explicitly‑named columns ---------------------------------------
    explicit_cols = [
        "COVID_date", 
        
        "visits_per_month",
        "age_at_covid",                    # keep the space if the column is literally named that;
                                            # otherwise change to "data_provider"
        "data_partner_id",
        "BMI_max_observed_or_calculated_before_or_day_of_covid",
        "TOBACCOSMOKER_before_or_day_of_covid_indicator",
        "OBESITY_before_or_day_of_covid_indicator",
        "CHRONICLUNGDISEASE_before_or_day_of_covid_indicator",
        "HYPERTENSION_before_or_day_of_covid_indicator",
        "DEPRESSION_before_or_day_of_covid_indicator",
        "SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator",
        "asthma",
        "hba1c",
        "insulin",
        "HEARTFAILURE_before_or_day_of_covid_indicator",
        "DEMENTIA_before_or_day_of_covid_indicator",
        "arthritis",
        "CORONARYARTERYDISEASE_before_or_day_of_covid_indicator",
        "MALIGNANTCANCER_before_or_day_of_covid_indicator",
        "METASTATICSOLIDTUMORCANCERS_before_or_day_of_covid_indicator",
        "MILDLIVERDISEASE_before_or_day_of_covid_indicator",
        "MODERATESEVERELIVERDISEASE_before_or_day_of_covid_indicator",
        "KIDNEYDISEASE_before_or_day_of_covid_indicator",
        "creatinine",
        "ace_inhibitors",
        "angiotensin",
        "statins",
        "anticoagulants",
        "aspirin",
        "visit_after_enrollment",
        "PERIPHERALVASCULARDISEASE_before_or_day_of_covid_indicator",
        "CEREBROVASCULARDISEASE_before_or_day_of_covid_indicator",
        "glomerular_filtration_rate",
        "albumin_creatinine_ratio",
        "furosemide",
        "torsemide",
        "months_from_2018",
        "pcos",
        "all_glp_1", 
        "death_within_12_months", 
        "long_covid_in_study_period", 
        "death_or_long_covid", 
        "autoimmune", 
        "immune_suppression", 
        "long_covid_phenotype"
 

    ]

    # 3) build the final list, keeping order but removing any accidental dups ----
    cols_to_select = []
    for c in prefix_cols + explicit_cols:
        if c not in cols_to_select:
            cols_to_select.append(c)

    # 4) perform the selection ---------------------------------------------------
    selected_df = Final_table_4.select(*[F.col(c) for c in cols_to_select])

    return selected_df

    

