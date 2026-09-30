

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.71bd0ed7-7b19-4b3c-94e4-b38f91de9269"),
    Final_Table_9=Input(rid="ri.foundry.main.dataset.faefaa6a-3e32-421b-ba69-8dd64ba1e48c"),
    model_scores_all=Input(rid="ri.foundry.main.dataset.c11501d5-4d0e-4c68-bc4a-882db27c9c82")
)
SELECT
  f.*,
  CASE WHEN EXISTS (
    SELECT 1
    FROM model_scores_all m
    WHERE m.person_id = f.person_id
      AND CAST(m.model_score AS DOUBLE) >= 0.9
      AND TO_DATE(m.window_start) BETWEEN TO_DATE(f.COVID_first_poslab_or_diagnosis_date)
                                     AND DATE_ADD(TO_DATE(f.COVID_first_poslab_or_diagnosis_date), 265)
  ) THEN 1 ELSE 0 END AS long_covid_phenotype
FROM Final_Table_9 f;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8c566b66-e944-46cc-8605-ea9475dbc76b"),
    Final_Table_1=Input(rid="ri.foundry.main.dataset.ad0c3e96-af8e-4b5d-8721-581870bed197"),
    latest_non_null_hba1c_1=Input(rid="ri.vector.main.execute.dd44d4c5-3b9a-4840-9437-cbaa1f3ac86e")
)
SELECT ft.*,
       lnh.harmonized_value_as_number AS hba1c
FROM   Final_Table_1            AS ft
LEFT   JOIN latest_non_null_hba1c_1 AS lnh
       ON  ft.person_id = lnh.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c5fb653a-2add-4c4f-9f67-36245ec57bab"),
    Final_Table_2=Input(rid="ri.foundry.main.dataset.8c566b66-e944-46cc-8605-ea9475dbc76b"),
    Healthcare_utilization=Input(rid="ri.foundry.main.dataset.73d59e15-08bc-4934-bd31-e0487a21c191")
)
SELECT  f.*,
        h.Outpatient_Visit,
        h.Inpatient_Visit
FROM    Final_Table_2          AS f
LEFT JOIN Healthcare_utilization AS h
       ON h.person_id = f.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.359823b5-495e-4e3a-a71a-eb52679a151b"),
    add_glomerular=Input(rid="ri.foundry.main.dataset.a6d8209b-e8e9-414c-88a2-5d0c902f91f9"),
    long_covid_filter=Input(rid="ri.foundry.main.dataset.73912bdf-ad1d-4c83-9496-41433fbd18fa"),
    ssri_filter=Input(rid="ri.foundry.main.dataset.db50c60b-34f4-438b-821d-4bebdc36e02f")
)
SELECT f.*
FROM add_glomerular as f LEFT JOIN ssri_filter as s
ON f.data_partner_id=s.data_partner_id
LEFT JOIN long_covid_filter as l
ON f.data_partner_id=l.data_partner_id
WHERE s.above_cutoff=1 AND l.above_cutoff=1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a0ea6762-89e7-492f-adfb-0bdb557031a5"),
    Final_Table_5=Input(rid="ri.foundry.main.dataset.f2ea8cd3-61e8-4681-a0ea-63082d962fae"),
    visit_occurrence_1=Input(rid="ri.foundry.main.dataset.3f74d43a-d981-4e17-93f0-21c811c57aab")
)
-- Spark SQL
WITH f AS (                      -- add “months since 2018-01-01”
    SELECT  *,
            (YEAR(drug_exposure_start_date) - 2018) * 12
          +  MONTH(drug_exposure_start_date) AS months_since_2018
    FROM    Final_Table_5
),
visit_cnt AS (                   -- one row per (person_id, drug_date)
    SELECT  f.person_id,
            f.drug_exposure_start_date,
            COUNT(DISTINCT vo.visit_occurrence_id) AS visit_cnt
    FROM    Final_Table_5 AS f
    LEFT JOIN visit_occurrence_1 AS vo
           ON  vo.person_id = f.person_id
          AND vo.visit_start_date <= f.drug_exposure_start_date
    GROUP BY f.person_id,
             f.drug_exposure_start_date
)

SELECT
    f.*,
    CASE 
        WHEN f.months_since_2018 <= 0 THEN NULL
        WHEN COALESCE(visit_cnt.visit_cnt, 0) / CAST(f.months_since_2018 AS DOUBLE) < 0 THEN NULL
        ELSE COALESCE(visit_cnt.visit_cnt, 0) / CAST(f.months_since_2018 AS DOUBLE)
    END AS visits_per_month
FROM   f
LEFT JOIN visit_cnt
       ON  f.person_id               = visit_cnt.person_id
      AND f.drug_exposure_start_date = visit_cnt.drug_exposure_start_date;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c37a5d26-2e3f-4f87-b03b-8882d401738b"),
    Final_Table_6=Input(rid="ri.foundry.main.dataset.a0ea6762-89e7-492f-adfb-0bdb557031a5")
)
SELECT
    ed.*,
    months_between(ed.COVID_first_poslab_or_diagnosis_date, DATE '2018-01-01')
        AS COVID_date
FROM  Final_Table_6 AS ed

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e0311527-06c7-48c7-b4cb-42aaa33ee5d0"),
    Final_Table_7=Input(rid="ri.foundry.main.dataset.c37a5d26-2e3f-4f87-b03b-8882d401738b"),
    all_patients_fact_day_table_LDS=Input(rid="ri.foundry.main.dataset.bb008308-b8d0-41b7-b112-49840247d31e")
)
WITH agg AS (
    SELECT /*+ BROADCAST(f) */
           f.person_id,
           f.drug_exposure_start_date,

           MAX(ap.BMI_rounded)                 AS BMI_max_observed_or_calculated_before_or_day_of_covid,
           MAX(ap.OBESITY)                     AS OBESITY_before_or_day_of_covid_indicator,
           MAX(ap.HYPERTENSION)                AS HYPERTENSION_before_or_day_of_covid_indicator,
           MAX(ap.HEARTFAILURE)                AS HEARTFAILURE_before_or_day_of_covid_indicator,
           MAX(ap.CORONARYARTERYDISEASE)       AS CORONARYARTERYDISEASE_before_or_day_of_covid_indicator,
           MAX(ap.CHRONICLUNGDISEASE)          AS CHRONICLUNGDISEASE_before_or_day_of_covid_indicator,
           MAX(ap.PERIPHERALVASCULARDISEASE)   AS PERIPHERALVASCULARDISEASE_before_or_day_of_covid_indicator,
           MAX(ap.CEREBROVASCULARDISEASE)      AS CEREBROVASCULARDISEASE_before_or_day_of_covid_indicator,
           MAX(ap.TOBACCOSMOKER)               AS TOBACCOSMOKER_before_or_day_of_covid_indicator,
           MAX(ap.DEPRESSION)                  AS DEPRESSION_before_or_day_of_covid_indicator,
           MAX(ap.SYSTEMICCORTICOSTEROIDS)     AS SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator,
           MAX(ap.DEMENTIA)                    AS DEMENTIA_before_or_day_of_covid_indicator,
           MAX(ap.MALIGNANTCANCER)             AS MALIGNANTCANCER_before_or_day_of_covid_indicator,
           MAX(ap.METASTATICSOLIDTUMORCANCERS) AS METASTATICSOLIDTUMORCANCERS_before_or_day_of_covid_indicator,
           MAX(ap.MILDLIVERDISEASE)            AS MILDLIVERDISEASE_before_or_day_of_covid_indicator,
           MAX(ap.MODERATESEVERELIVERDISEASE)  AS MODERATESEVERELIVERDISEASE_before_or_day_of_covid_indicator,
           MAX(ap.KIDNEYDISEASE)               AS KIDNEYDISEASE_before_or_day_of_covid_indicator
    FROM   Final_Table_7  f
    JOIN   all_patients_fact_day_table_LDS ap
           ON ap.person_id = f.person_id
          AND ap.date      <= f.drug_exposure_start_date
    GROUP  BY f.person_id,
             f.drug_exposure_start_date
)

SELECT
    f.* ,                      -- keys + original columns (only once!)
    a.BMI_max_observed_or_calculated_before_or_day_of_covid ,
    a.OBESITY_before_or_day_of_covid_indicator ,
    a.HYPERTENSION_before_or_day_of_covid_indicator ,
    a.HEARTFAILURE_before_or_day_of_covid_indicator ,
    a.CORONARYARTERYDISEASE_before_or_day_of_covid_indicator ,
    a.CHRONICLUNGDISEASE_before_or_day_of_covid_indicator ,
    a.PERIPHERALVASCULARDISEASE_before_or_day_of_covid_indicator ,
    a.CEREBROVASCULARDISEASE_before_or_day_of_covid_indicator ,
    a.TOBACCOSMOKER_before_or_day_of_covid_indicator ,
    a.DEPRESSION_before_or_day_of_covid_indicator ,
    a.SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator ,
    a.DEMENTIA_before_or_day_of_covid_indicator ,
    a.MALIGNANTCANCER_before_or_day_of_covid_indicator ,
    a.METASTATICSOLIDTUMORCANCERS_before_or_day_of_covid_indicator ,
    a.MILDLIVERDISEASE_before_or_day_of_covid_indicator ,
    a.MODERATESEVERELIVERDISEASE_before_or_day_of_covid_indicator ,
    a.KIDNEYDISEASE_before_or_day_of_covid_indicator
FROM    Final_Table_7 f
LEFT    JOIN agg a
       ON f.person_id               = a.person_id
      AND f.drug_exposure_start_date = a.drug_exposure_start_date;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.faefaa6a-3e32-421b-ba69-8dd64ba1e48c"),
    add_autoimmune=Input(rid="ri.foundry.main.dataset.b28f3212-9ff7-4eec-ab09-67ce2bd27dbb"),
    index_immune=Input(rid="ri.foundry.main.dataset.b212d687-80a1-4a34-8684-33ea73922ac6")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   index_immune ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS immune_suppression
FROM add_autoimmune aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0c01d340-f3de-480d-9ab1-3ffeb5a2b1fb"),
    drug_exposure_1=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM   drug_exposure_1
WHERE  drug_concept_id IN (
    19040051,
    1363749,
    19122327,
    1342439,
    1335471,
    19102107,
    1334456,
    1308216,
    1373225,
    1331235,
    1310756,
    1340128,
    1341927,
    19050216
);

-- https://unite.nih.gov/workspace/hubble/objects/ri.phonograph2-objects.main.object.c543cdd5-172e-4c94-b520-2abc7bec2cfc

@transform_pandas(
    Output(rid="ri.vector.main.execute.f088a1b2-0c6c-49e0-b353-9aef1449e593"),
    ace_exposures=Input(rid="ri.foundry.main.dataset.0c01d340-f3de-480d-9ab1-3ffeb5a2b1fb"),
    add_pcos=Input(rid="ri.foundry.main.dataset.f4312e33-29af-43b9-bbab-34dfcc45f081")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    ace_exposures AS p
JOIN    add_pcos   AS a
        ON p.person_id = a.person_id
WHERE   p.drug_exposure_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.vector.main.execute.a740df95-4d1e-4d04-8616-562bfa36cb45"),
    ace_indexed=Input(rid="ri.vector.main.execute.f088a1b2-0c6c-49e0-b353-9aef1449e593"),
    add_insulin=Input(rid="ri.vector.main.execute.637a4bb1-67ae-4ba2-a529-6f8901dcb132")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   ace_indexed ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS ace_inhibitors
FROM add_insulin aa;

@transform_pandas(
    Output(rid="ri.vector.main.execute.5b40a9ec-0d6b-404e-bf98-d75bd4f8bb71"),
    add_furosemide=Input(rid="ri.foundry.main.dataset.50b4d888-feac-4204-be51-eada2fe41d07"),
    latest_non_null_albumin=Input(rid="ri.vector.main.execute.cfb38111-64b7-4084-abde-233f847d972d")
)
SELECT ft.*,
       lnh.value_as_number AS albumin_creatinine_ratio
FROM   add_furosemide            AS ft
LEFT   JOIN latest_non_null_albumin AS lnh
       ON  ft.person_id = lnh.person_id;

@transform_pandas(
    Output(rid="ri.vector.main.execute.0a5efbec-ac2a-495a-b7a2-79d83c4d4e28"),
    add_ace=Input(rid="ri.vector.main.execute.a740df95-4d1e-4d04-8616-562bfa36cb45"),
    angiotensin_indexed=Input(rid="ri.vector.main.execute.67cfee43-2929-4956-8f8c-ce6101d81537")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   angiotensin_indexed ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS angiotensin
FROM add_ace aa;

@transform_pandas(
    Output(rid="ri.vector.main.execute.e26ac310-02b7-4fcd-96c0-5e1af2edf2f8"),
    add_statins=Input(rid="ri.vector.main.execute.b67f0422-a9bb-413e-89af-bc4b4d3d2d21"),
    anticoagulants_indexed=Input(rid="ri.vector.main.execute.a96c9948-dd1c-43a6-b67f-6347c7485c92")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   anticoagulants_indexed ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS anticoagulants
FROM add_statins aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cafa4915-6dee-4607-b376-fe5e6de498d7"),
    add_asthma=Input(rid="ri.foundry.main.dataset.ad5ebdab-4edf-46de-8f0b-7985b0f96e6d"),
    arthritis_indexed=Input(rid="ri.foundry.main.dataset.401de0c3-98d4-49c8-8cad-e8f69d86820b")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (               -- does this person_id appear in arthritis_indexed?
            SELECT 1
            FROM   arthritis_indexed ai
            WHERE  ai.person_id = aa.person_id
        )
        THEN 1                      -- match found  → arthritis = 1
        ELSE 0                      -- no match     → arthritis = 0
    END AS arthritis
FROM add_asthma aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ab5e5ab-1955-48d2-997a-06c297681d0f"),
    add_anticoagulants=Input(rid="ri.vector.main.execute.e26ac310-02b7-4fcd-96c0-5e1af2edf2f8"),
    aspirin_indexed=Input(rid="ri.vector.main.execute.c517f49d-a5a6-42cf-8c57-07cb84abbdfc")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   aspirin_indexed ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS aspirin
FROM add_anticoagulants aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ad5ebdab-4edf-46de-8f0b-7985b0f96e6d"),
    asthma_indexed=Input(rid="ri.foundry.main.dataset.aa0b6f86-b22c-4c96-901d-6eeb92ed48a1"),
    merge_drug_exposure_date=Input(rid="ri.foundry.main.dataset.fc01cfae-b2ce-44b0-86bc-3eff4006c987")
)
SELECT
    ei.*,
    CASE
        WHEN EXISTS (   -- does this person_id appear in asthma_indexed?
            SELECT 1
            FROM   asthma_indexed ai
            WHERE  ai.person_id = ei.person_id
        )
        THEN 1          -- yes → mark as asthma = 1
        ELSE 0          -- no  → asthma = 0
    END AS asthma
FROM merge_drug_exposure_date ei;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b28f3212-9ff7-4eec-ab09-67ce2bd27dbb"),
    Final_Table_8=Input(rid="ri.foundry.main.dataset.e0311527-06c7-48c7-b4cb-42aaa33ee5d0"),
    index_autoimmune=Input(rid="ri.foundry.main.dataset.c2097aa4-00bf-4010-b1e1-fe5007826c86")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   index_autoimmune ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS autoimmune
FROM Final_Table_8 aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ce006b9-c350-42fc-988d-7cf326d28575"),
    add_long_covid_flag=Input(rid="ri.vector.main.execute.80de645d-9b5e-444c-8662-325ca021f566"),
    most_recent_measurement=Input(rid="ri.vector.main.execute.a8b649f9-ceb0-4e65-86c9-c7d34e625e5d")
)
-- Attach each person’s most‑recent creatinine value (if any)
SELECT  a.*,
        m.harmonized_value_as_number AS creatinine       -- new column
FROM    add_long_covid_flag     AS a
LEFT JOIN most_recent_measurement AS m      -- 1 row per person_id
       ON a.person_id = m.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dad43d62-bf21-46b7-8a55-92828c7d04cc"),
    Enriched_Death_Table=Input(rid="ri.foundry.main.dataset.b07b1c8f-97a7-43b4-826a-299e456c6e85"),
    add_creatinine=Input(rid="ri.foundry.main.dataset.4ce006b9-c350-42fc-988d-7cf326d28575")
)
SELECT w.*,
       CASE 
         WHEN EXISTS (
           SELECT 1 
           FROM Enriched_Death_Table d
           WHERE d.person_id = w.person_id
             AND d.death_date IS NOT NULL
             AND d.death_date BETWEEN w.COVID_first_poslab_or_diagnosis_date 
                                  AND DATEADD(MONTH, 12, w.COVID_first_poslab_or_diagnosis_date)
         ) THEN 1 
         ELSE 0 
       END AS death_within_12_months
FROM add_creatinine w;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.69242315-13c6-45c4-ac51-2db1f143fc01"),
    add_death=Input(rid="ri.foundry.main.dataset.dad43d62-bf21-46b7-8a55-92828c7d04cc")
)
SELECT  d.*,
        CASE 
            WHEN d.death_within_12_months = 1 
                 OR d.long_covid_in_study_period = 1
            THEN 1
            ELSE 0
        END AS death_or_long_covid
FROM    add_death AS d;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d5e75fc3-8bea-4b48-85d7-d646edc7df0f"),
    add_metformin_dates=Input(rid="ri.foundry.main.dataset.2f40ffca-0ef1-46cf-84ff-a469c7543dd0"),
    dpp4i_indexed=Input(rid="ri.foundry.main.dataset.3aa70fcf-8d2c-418f-9a83-d4eb43b05587")
)
SELECT
    ed.*,
    CASE 
        WHEN mi.person_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS dpp4i
FROM  add_metformin_dates           AS ed
LEFT JOIN (SELECT DISTINCT person_id FROM dpp4i_indexed) AS mi
       ON ed.person_id = mi.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.82644e31-383c-4986-a7d4-ad98abd90711"),
    add_dpp4i=Input(rid="ri.foundry.main.dataset.d5e75fc3-8bea-4b48-85d7-d646edc7df0f"),
    dpp4i_dates=Input(rid="ri.foundry.main.dataset.c1e888ab-f5e1-40f0-b4ae-116de790114a")
)
SELECT
    a.*,                                       -- all existing columns
    m.drug_exposure_start_date
        AS dpp4i_drug_exposure_start_date  -- new column
FROM add_dpp4i            AS a
LEFT JOIN dpp4i_dates AS m
       ON a.person_id = m.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.50b4d888-feac-4204-be51-eada2fe41d07"),
    add_torsemide=Input(rid="ri.foundry.main.dataset.b8cabef6-db48-4d64-a4cf-174aee2ccaa7"),
    index_furosemide=Input(rid="ri.vector.main.execute.4a801443-5a5e-4188-9d1e-a2b9d3ea526f")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   index_furosemide ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS furosemide
FROM add_torsemide aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a6d8209b-e8e9-414c-88a2-5d0c902f91f9"),
    add_albumin=Input(rid="ri.vector.main.execute.5b40a9ec-0d6b-404e-bf98-d75bd4f8bb71"),
    latest_non_null_glomerular=Input(rid="ri.vector.main.execute.6d120135-6c2c-4cd0-b1e1-027f3dfe2eb3")
)
SELECT ft.*,
       lnh.value_as_number AS glomerular_filtration_rate
FROM   add_albumin            AS ft
LEFT   JOIN latest_non_null_glomerular AS lnh
       ON  ft.person_id = lnh.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f715a27c-515c-4dd3-8bdf-3daac1695526"),
    add_t2dm=Input(rid="ri.foundry.main.dataset.125153de-b163-4ea6-addc-393a96dc1219"),
    latest_non_null_hba1c=Input(rid="ri.foundry.main.dataset.b5109cfd-63b5-4d83-82e1-06105066e39c")
)
SELECT
    ad.*, wp.hba1c_t2dm, 
    /* … */
    /* combined flag */
    CASE
        WHEN COALESCE(ad.t2dm, 0) = 1
          OR COALESCE(wp.hba1c_t2dm, 0) = 1
        THEN 1
        ELSE 0
    END AS t2dm_combined
FROM   add_t2dm      AS ad
LEFT   JOIN latest_non_null_hba1c AS wp
       ON ad.person_id = wp.person_id;

@transform_pandas(
    Output(rid="ri.vector.main.execute.637a4bb1-67ae-4ba2-a529-6f8901dcb132"),
    add_pcos=Input(rid="ri.foundry.main.dataset.f4312e33-29af-43b9-bbab-34dfcc45f081"),
    insulin_indexed=Input(rid="ri.vector.main.execute.f7fc25e1-4bc3-4727-86ab-4cff63516639")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   insulin_indexed ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS insulin
FROM add_pcos aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dc888d77-b2cd-49a5-b338-350e39a25f36"),
    age_filter=Input(rid="ri.foundry.main.dataset.61e2295f-1241-46d2-9718-31ef2560cc11"),
    kidney_before_covid=Input(rid="ri.foundry.main.dataset.e7d7a562-6e90-42ea-a004-49f348d0b94b")
)
SELECT
    a.*,
    CASE
        WHEN EXISTS ( SELECT 1
                      FROM   kidney_before_covid AS p
                      WHERE  p.person_id = a.person_id )
        THEN 1         -- person_id is in the pre‑diabetes table
        ELSE 0         -- person_id is not
    END AS kidney_disease_stage_3_4_5
FROM age_filter AS a;

@transform_pandas(
    Output(rid="ri.vector.main.execute.80de645d-9b5e-444c-8662-325ca021f566"),
    add_aspirin=Input(rid="ri.foundry.main.dataset.4ab5e5ab-1955-48d2-997a-06c297681d0f"),
    long_covid_indexed=Input(rid="ri.vector.main.execute.98800a85-12f4-435a-90d7-9615cbe60a8d")
)
-- Add a flag that is 1 if the person appears in long_covid_indexed, else 0
SELECT  a.*,
        CASE WHEN l.person_id IS NOT NULL THEN 1 ELSE 0 END AS long_covid_in_study_period
FROM    add_aspirin AS a
LEFT JOIN (SELECT DISTINCT person_id FROM long_covid_indexed) AS l
       ON a.person_id = l.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0409852e-1fae-4d84-bd0b-eb1718b127bf"),
    exclude_diseases=Input(rid="ri.foundry.main.dataset.6ff8e885-c657-46b2-9dbb-28a807927677")
)
SELECT
    ed.*
FROM  exclude_diseases           AS ed

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2f40ffca-0ef1-46cf-84ff-a469c7543dd0"),
    add_metformin=Input(rid="ri.foundry.main.dataset.0409852e-1fae-4d84-bd0b-eb1718b127bf")
)
SELECT
    a.*
FROM add_metformin            AS a

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f4312e33-29af-43b9-bbab-34dfcc45f081"),
    add_arthritis=Input(rid="ri.foundry.main.dataset.cafa4915-6dee-4607-b376-fe5e6de498d7"),
    pcos_indexed=Input(rid="ri.vector.main.execute.d40a0434-2a36-4576-8c6c-3a86755e89ea")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (               -- does this person_id appear in arthritis_indexed?
            SELECT 1
            FROM   pcos_indexed ai
            WHERE  ai.person_id = aa.person_id
        )
        THEN 1                      -- match found  → arthritis = 1
        ELSE 0                      -- no match     → arthritis = 0
    END AS pcos
FROM add_arthritis aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.839cff9e-9ac0-4e3e-89fb-2869eddac98d"),
    add_kidney=Input(rid="ri.foundry.main.dataset.dc888d77-b2cd-49a5-b338-350e39a25f36"),
    renal_before_covid=Input(rid="ri.foundry.main.dataset.6e61fd16-6a76-4af3-8aa6-2857fd389494")
)
SELECT
    a.*,
    CASE
        WHEN EXISTS ( SELECT 1
                      FROM   renal_before_covid AS p
                      WHERE  p.person_id = a.person_id )
        THEN 1         -- person_id is in the pre‑diabetes table
        ELSE 0         -- person_id is not
    END AS renal_disease
FROM add_kidney AS a;

@transform_pandas(
    Output(rid="ri.vector.main.execute.b67f0422-a9bb-413e-89af-bc4b4d3d2d21"),
    add_angiotensin=Input(rid="ri.vector.main.execute.0a5efbec-ac2a-495a-b7a2-79d83c4d4e28"),
    statins_indexed=Input(rid="ri.vector.main.execute.40b96995-57df-4be7-9a41-99ea8b24ad6c")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   statins_indexed ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS statins
FROM add_angiotensin aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.66fa4716-66b7-4649-97eb-59f92cee8f02"),
    add_dpp4i_dates=Input(rid="ri.foundry.main.dataset.82644e31-383c-4986-a7d4-ad98abd90711"),
    su_indexed=Input(rid="ri.foundry.main.dataset.0efb3d6b-3e7b-430b-99bb-fa5d0be41773")
)
SELECT
    ed.*,
    CASE 
        WHEN mi.person_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS all_glp_1
FROM  add_dpp4i_dates           AS ed
LEFT JOIN (SELECT DISTINCT person_id FROM su_indexed) AS mi
       ON ed.person_id = mi.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.909acec3-ea58-4929-8798-746bea2a06eb"),
    add_su=Input(rid="ri.foundry.main.dataset.66fa4716-66b7-4649-97eb-59f92cee8f02"),
    su_dates=Input(rid="ri.foundry.main.dataset.13b7ccdc-8c52-4d56-b92d-c666eb372c38")
)
SELECT
    a.*,                                       -- all existing columns
    m.drug_exposure_start_date
        AS all_glp_1_drug_exposure_start_date  -- new column
FROM add_su            AS a
LEFT JOIN su_dates AS m
       ON a.person_id = m.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.125153de-b163-4ea6-addc-393a96dc1219"),
    add_renal=Input(rid="ri.foundry.main.dataset.839cff9e-9ac0-4e3e-89fb-2869eddac98d"),
    t2dm_before_covid=Input(rid="ri.foundry.main.dataset.d225c401-c669-45b5-b873-488690b138d9")
)
SELECT
    a.*,
    CASE
        WHEN EXISTS ( SELECT 1
                      FROM   t2dm_before_covid AS p
                      WHERE  p.person_id = a.person_id )
        THEN 1         -- person_id is in the pre‑diabetes table
        ELSE 0         -- person_id is not
    END AS t2dm
FROM add_renal AS a;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b8cabef6-db48-4d64-a4cf-174aee2ccaa7"),
    Final_Table_3=Input(rid="ri.foundry.main.dataset.c5fb653a-2add-4c4f-9f67-36245ec57bab"),
    index_torsemide=Input(rid="ri.vector.main.execute.1b0e7ede-65fe-4b22-a121-5f9966349cf5")
)
SELECT
    aa.*,
    CASE
        WHEN EXISTS (          -- is this person_id in insulin_indexed?
            SELECT 1
            FROM   index_torsemide ii
            WHERE  ii.person_id = aa.person_id
        )
        THEN 1                 -- yes → insulin = 1
        ELSE 0                 -- no  → insulin = 0
    END AS torsemide
FROM Final_Table_3 aa;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.61e2295f-1241-46d2-9718-31ef2560cc11"),
    filter_covid_date=Input(rid="ri.foundry.main.dataset.aaafaba7-a913-4f88-986f-638c502f90ee")
)
SELECT *
FROM filter_covid_date
WHERE age_at_covid >= 30 AND age_at_covid <= 85; 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.77d37e09-53c7-4685-a978-1766e77cbb72"),
    concept_set_members_1=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members_1
WHERE codeset_id = 884569009

@transform_pandas(
    Output(rid="ri.vector.main.execute.8cbd8ad7-4394-4376-8a0d-f46ca7af7412"),
    albumin_concepts=Input(rid="ri.foundry.main.dataset.77d37e09-53c7-4685-a978-1766e77cbb72"),
    measurement_3=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
SELECT *
FROM measurement_3 INNER JOIN albumin_concepts
ON measurement_3.measurement_concept_id = albumin_concepts.concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.02163d31-95aa-4d35-adf2-a24ccfe0f0ce"),
    drug_exposure_1=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM   drug_exposure_1
WHERE  drug_concept_id IN (
    1317640,
    1351557,
    1346686,
    40226742,
    1367500,
    1347384,
    1308842,
    40235485
);

-- https://unite.nih.gov/workspace/hubble/objects/ri.phonograph2-objects.main.object.432c499f-0d52-43fb-86eb-132439f68f42

@transform_pandas(
    Output(rid="ri.vector.main.execute.67cfee43-2929-4956-8f8c-ce6101d81537"),
    add_pcos=Input(rid="ri.foundry.main.dataset.f4312e33-29af-43b9-bbab-34dfcc45f081"),
    angiotensin_exposures=Input(rid="ri.foundry.main.dataset.02163d31-95aa-4d35-adf2-a24ccfe0f0ce")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    angiotensin_exposures AS p
JOIN    add_pcos   AS a
        ON p.person_id = a.person_id
WHERE   p.drug_exposure_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4adb0723-e487-42d1-a17a-41bbe8fcac80"),
    drug_exposure_1=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM   drug_exposure_1
WHERE  drug_concept_id IN (
    35806286,
    1301025,
    35806274,
    35804520,
    35806332,
    43013024,
    45892847,
    1310149,
    35806284,
    35806268,
    35806856,
    40241331,
    42542407,
    35806054,
    35806273,
    35804562,
    35805338,
    35806053,
    35806051,
    35805370,
    35805045,
    35806285,
    45775372,
    35803596,
    35806061,
    40228152,
    35806317
);

-- https://unite.nih.gov/workspace/hubble/objects/ri.phonograph2-objects.main.object.8e962b04-cf4e-4302-b664-6832241b4fed

@transform_pandas(
    Output(rid="ri.vector.main.execute.a96c9948-dd1c-43a6-b67f-6347c7485c92"),
    add_pcos=Input(rid="ri.foundry.main.dataset.f4312e33-29af-43b9-bbab-34dfcc45f081"),
    anticoagulants_exposures=Input(rid="ri.foundry.main.dataset.4adb0723-e487-42d1-a17a-41bbe8fcac80")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    anticoagulants_exposures AS p
JOIN    add_pcos   AS a
        ON p.person_id = a.person_id
WHERE   p.drug_exposure_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.401de0c3-98d4-49c8-8cad-e8f69d86820b"),
    arthritis_occurrence=Input(rid="ri.foundry.main.dataset.1b5bf09f-5a0b-49d1-b9b9-588311baf378"),
    merge_drug_exposure_date=Input(rid="ri.foundry.main.dataset.fc01cfae-b2ce-44b0-86bc-3eff4006c987")
)
SELECT  p.*, a.drug_exposure_start_date
FROM    arthritis_occurrence AS p
JOIN    merge_drug_exposure_date             AS a
        ON p.person_id = a.person_id
WHERE   p.condition_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1b5bf09f-5a0b-49d1-b9b9-588311baf378"),
    condition_occurrence_1=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT *
FROM condition_occurrence_1
WHERE condition_concept_id IN (
    37207813, 601040, 601048, 42534836, 601047, 4117687, 36686999, 36683391,
    601041, 4116445, 4297649, 4311391, 4147418, 4184896, 601045, 4299308,
    37207810, 4115051, 37209321, 601044, 40400211, 4297651, 608811, 35609009,
    601038, 36685023, 601035, 4115050, 601042, 36685019, 4116440, 601039,
    36687003, 603309, 4270869, 4103516, 4116148, 81097, 4200987, 607145,
    4114439, 601037, 4243509, 37108591, 4300202, 37207814, 4116152, 42534837,
    37209323, 601032, 609024, 608041, 36687000, 601046, 603298, 4269880,
    608812, 4114440, 36687001, 4299403, 603308, 4116442, 37108590, 4347065,
    601030, 4292371, 608809, 609061, 4296152, 4116149, 36685022, 36685017,
    4179536, 36684998, 4142899, 4116444, 37209312, 4116151, 80809, 601033,
    4162539, 4132809, 608813, 37207806, 42534834, 37209311, 608810, 4083556,
    37108714, 4116150, 37207815, 608042, 37207809, 4116153, 4114441, 605413,
    36687002, 37207807, 601034, 4116443, 36685021, 37209322, 37207804,
    4030424, 601036, 603300, 607144, 603302, 4114442, 35609010, 4035611,
    4116446, 37207805, 603299, 37117421, 4028118, 601031, 36687006, 37207808,
    4114444, 4306357, 36685018, 256197, 4334806, 603301, 4271003, 42539550,
    37207812, 605414, 4115161, 4297650, 4179378, 601043, 4116441, 42534835
);

-- https://unite.nih.gov/workspace/hubble/objects/ri.phonograph2-objects.main.object.618ca91e-214f-4795-b6c9-60d5d78314b7

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1e11fd37-07ff-4df4-a844-d5e786f0cd63"),
    drug_exposure_1=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM drug_exposure_1
WHERE drug_concept_id = 1112807

-- https://unite.nih.gov/workspace/hubble/objects/ri.phonograph2-objects.main.object.68143abd-33df-4fe4-9d70-2c9f037a41c6

@transform_pandas(
    Output(rid="ri.vector.main.execute.c517f49d-a5a6-42cf-8c57-07cb84abbdfc"),
    add_pcos=Input(rid="ri.foundry.main.dataset.f4312e33-29af-43b9-bbab-34dfcc45f081"),
    aspirin_exposures=Input(rid="ri.foundry.main.dataset.1e11fd37-07ff-4df4-a844-d5e786f0cd63")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    aspirin_exposures AS p
JOIN    add_pcos   AS a
        ON p.person_id = a.person_id
WHERE   p.drug_exposure_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aa0b6f86-b22c-4c96-901d-6eeb92ed48a1"),
    asthma_occurrences=Input(rid="ri.foundry.main.dataset.9e5745b1-37ad-40e1-adb4-665cd0bab95d"),
    merge_drug_exposure_date=Input(rid="ri.foundry.main.dataset.fc01cfae-b2ce-44b0-86bc-3eff4006c987")
)
SELECT  p.*, a.drug_exposure_start_date
FROM    asthma_occurrences AS p
JOIN    merge_drug_exposure_date             AS a
        ON p.person_id = a.person_id
WHERE   p.condition_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9e5745b1-37ad-40e1-adb4-665cd0bab95d"),
    condition_occurrence_1=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
-- keep only the rows whose condition_concept_id is in the list
SELECT *
FROM   condition_occurrence_1
WHERE  condition_concept_id IN (
    43530693, 4207479, 46269771, 4191479, 4206340, 4145497, 37116845,
    4146581, 4232595, 36684328, 4191827, 4152418, 45769441, 4152292,
    4110051, 46269774, 764677, 46270028, 4051466, 4152420, 42539549,
    4015819, 4155470, 4142738, 4301938, 4017183, 46274062, 443801,
    4015947, 761844, 46269785, 46273454, 45768965, 4271333, 4245676,
    4211530, 45768964, 45766728, 45768911, 40483397, 4143828, 4152913,
    4123253, 46269776, 46269786, 4017026, 4155469, 46270029, 45768963,
    45769442, 42536208, 4308356, 37108581, 4017025, 4143474, 46269802,
    45772937, 4225553, 40481763, 46273452, 45766727, 4080516, 45769352,
    43530745, 46274060, 4194289, 45772073, 4265861, 45769350, 4309833,
    37310241, 46269779, 46269784, 42536207, 256448, 46269778, 46269790,
    46269781, 4017184, 46269767, 257581, 46274124, 46269775, 46269801,
    46269773, 45768910, 4138760, 45769443, 4233784, 46269770, 46269788,
    46270030, 46269777, 4120261, 4057952, 46269783, 312950, 45768912,
    37109103, 46273635, 4156136, 46270573, 46274059, 764949, 313236,
    4119298, 4017182, 45769351, 4152911, 37108580, 37206717, 46273462,
    4155473, 46269789, 4250128, 46270322, 4212099, 46269782, 317009,
    4022592, 42535716, 4225554, 46269780, 36684335, 4161595, 46269772,
    3661412, 4245292, 36674599, 4075237, 4244339, 4141978, 252658,
    4145356, 45773005, 45769438, 46269787, 4119300, 4217558, 42538744,
    42536649, 4155468, 4017293
);

-- https://unite.nih.gov/workspace/hubble/objects/ri.phonograph2-objects.main.object.ebc613f3-c970-4b24-93bc-6d15df27e36b

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aa1b808d-097d-4256-b838-41ec2e00275c"),
    condition_occurrence_3=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT *
FROM condition_occurrence_3
WHERE condition_concept_id IN (435224, 433740, 439727, 434621);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6463fed3-9d6b-4343-8219-c5187068e8dc"),
    measurement_1=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
SELECT *
FROM   measurement_1
WHERE  measurement_concept_id IN (
    37392176,
    2212294,
    3016723,
    40482653,
    4150621,
    4199025,
    3007760,
    40485486,
    3051825,
    4155367,
    4013964
);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f4d2580a-fd26-4e65-b432-bbe67e656668"),
    interventions_after_covid=Input(rid="ri.foundry.main.dataset.83270446-9c8a-40ae-b1bd-80ed8a345248")
)
SELECT a.*
FROM interventions_after_covid AS a

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8cbad836-5750-4e94-8b98-e0d3ad6e9acb"),
    add_glomerular=Input(rid="ri.foundry.main.dataset.a6d8209b-e8e9-414c-88a2-5d0c902f91f9")
)
SELECT data_partner_id, COUNT(*) as Total_patients, SUM(all_glp_1) as SSRI_count, SUM(long_covid_in_study_period) as Long_COVID_count, (SUM(all_glp_1))/COUNT(*) as SSRI_proportion, SUM(long_covid_in_study_period)/COUNT(*) as Long_COVID_proportion
FROM add_glomerular
GROUP BY data_partner_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dc9fa741-13fc-4b26-9431-38a6eb8ca424"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM drug_exposure
WHERE drug_concept_id IN (
    1123627,
    40164891,
    19125045,
    42708175,
    19125043,
    19125047,
    19125049,
    45791817,
    40164922,
    42708167,
    42708171,
    793293,
    19125041,
    19125051,
    42708174,
    40164892,
    1580747,
    42708170,
    44785829,
    40164923,
    43526465,
    42708166
);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1e888ab-f5e1-40f0-b4ae-116de790114a"),
    dpp4i_indexed=Input(rid="ri.foundry.main.dataset.3aa70fcf-8d2c-418f-9a83-d4eb43b05587")
)
SELECT  *
FROM   (
        SELECT  m.*,
                ROW_NUMBER() OVER (PARTITION BY person_id
                                   ORDER BY drug_exposure_start_date DESC) AS rn
        FROM    dpp4i_indexed AS m
       ) t
WHERE  rn = 1;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3aa70fcf-8d2c-418f-9a83-d4eb43b05587"),
    dpp4i_administrations=Input(rid="ri.foundry.main.dataset.dc9fa741-13fc-4b26-9431-38a6eb8ca424"),
    exclude_diseases=Input(rid="ri.foundry.main.dataset.6ff8e885-c657-46b2-9dbb-28a807927677")
)
SELECT ma.*, ed.COVID_first_poslab_or_diagnosis_date
FROM   dpp4i_administrations AS ma
JOIN   exclude_diseases          AS ed
       ON ma.person_id = ed.person_id
WHERE  ma.drug_exposure_start_date <= date_sub(ed.COVID_first_poslab_or_diagnosis_date, 30)
  AND (ma.drug_exposure_end_date IS NULL
       OR ma.drug_exposure_end_date >= ed.COVID_first_poslab_or_diagnosis_date);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6ff8e885-c657-46b2-9dbb-28a807927677"),
    get_t2dm_patients=Input(rid="ri.foundry.main.dataset.4dcae842-6e41-496a-bde9-85ac4f7585d8")
)
SELECT *
FROM get_t2dm_patients
WHERE kidney_disease_stage_3_4_5 = 0 AND renal_disease = 0

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.acb4905f-054f-4953-a50f-c3bddd888261"),
    add_su_dates=Input(rid="ri.foundry.main.dataset.909acec3-ea58-4929-8798-746bea2a06eb")
)
SELECT *
FROM add_su_dates
WHERE COALESCE(all_glp_1, 0) + COALESCE(dpp4i, 0) = 1;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aaafaba7-a913-4f88-986f-638c502f90ee"),
    Covid_patient_summary_table_lds_v166_redo=Input(rid="ri.foundry.main.dataset.b61ca572-0131-43d8-9327-8c59464887bf")
)
SELECT person_id, sex, race, race_ethnicity, age_at_covid, data_partner_id, COVID_first_poslab_or_diagnosis_date
FROM Covid_patient_summary_table_lds_v166_redo
WHERE COVID_first_poslab_or_diagnosis_date>='2021-10-01' AND COVID_first_poslab_or_diagnosis_date<='2023-04-01'; 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4b8ac33b-3d8d-46bd-b5ba-031daef845e1"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members
WHERE codeset_id=79303843 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.219f2691-9a75-447d-bb1b-5bdd4c91115f"),
    drug_exposure_2=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da"),
    furosemide_concepts=Input(rid="ri.foundry.main.dataset.4b8ac33b-3d8d-46bd-b5ba-031daef845e1")
)
SELECT codeset_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_concept_name
FROM drug_exposure_2 INNER JOIN furosemide_concepts
ON drug_exposure_2.drug_concept_id = furosemide_concepts.concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4dcae842-6e41-496a-bde9-85ac4f7585d8"),
    add_hba1c=Input(rid="ri.foundry.main.dataset.f715a27c-515c-4dd3-8bdf-3daac1695526")
)
SELECT *
FROM add_hba1c
WHERE t2dm_combined = 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b3963d5d-8fce-42d5-8ced-9a9e5f82b2a3"),
    concept_set_members_1=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members_1
WHERE codeset_id = 509778716

@transform_pandas(
    Output(rid="ri.vector.main.execute.a1d034f6-bd38-4a0b-8114-133ea7658d25"),
    glomerular_concepts=Input(rid="ri.foundry.main.dataset.b3963d5d-8fce-42d5-8ced-9a9e5f82b2a3"),
    measurement_3=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
SELECT *
FROM measurement_3 INNER JOIN glomerular_concepts
ON measurement_3.measurement_concept_id = glomerular_concepts.concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.caba1e53-4e15-4460-99ae-a6f14c1d0397"),
    measurement=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
-- Rows in measurement_1 whose measurement_concept_id matches any ID in the list
SELECT *
FROM   measurement
WHERE  measurement_concept_id IN (
    2106236, 2212168, 36304734, 37116827, 37395558, 40765129, 42741295, 2212165,
    3005131, 4197971, 43527958, 709959, 709960, 2212166, 2617505, 3005673, 3007263,
    3033145, 37392407, 37398434, 4306438, 2212167, 40483736, 4235420, 9225, 2106238,
    2212393, 3003309, 3039720, 36032094, 36714633, 4276582, 42869630, 37392967,
    40217293, 4184637, 2617506, 36032066, 37392966, 37393623, 40486409, 764125,
    3004410, 37208641, 4306250, 44793001, 764124, 2212392, 37395559, 40762352,
    4152671, 4306587
);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.11523ac0-f464-4ac8-a1f0-57381fc2e20d"),
    measurement_2=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
-- Rows in measurement_1 whose measurement_concept_id matches any ID in the list
SELECT *
FROM   measurement_2
WHERE  measurement_concept_id IN (
    2106236, 2212168, 36304734, 37116827, 37395558, 40765129, 42741295, 2212165,
    3005131, 4197971, 43527958, 709959, 709960, 2212166, 2617505, 3005673, 3007263,
    3033145, 37392407, 37398434, 4306438, 2212167, 40483736, 4235420, 9225, 2106238,
    2212393, 3003309, 3039720, 36032094, 36714633, 4276582, 42869630, 37392967,
    40217293, 4184637, 2617506, 36032066, 37392966, 37393623, 40486409, 764125,
    3004410, 37208641, 4306250, 44793001, 764124, 2212392, 37395559, 40762352,
    4152671, 4306587
);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.98439c3b-8283-4a0a-9319-0b3aab1b63e0"),
    condition_occurrence_3=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    drug_exposure_3=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
SELECT *
FROM condition_occurrence_3
WHERE condition_concept_id IN (
    4242843
);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2eff562a-d0a6-4ef0-bd31-653207e778ed"),
    add_furosemide=Input(rid="ri.foundry.main.dataset.50b4d888-feac-4204-be51-eada2fe41d07"),
    albumin_measures=Input(rid="ri.vector.main.execute.8cbd8ad7-4394-4376-8a0d-f46ca7af7412")
)
SELECT hm.*
FROM   albumin_measures  AS hm
JOIN   add_furosemide   AS ft
       ON  hm.person_id = ft.person_id
WHERE  hm.measurement_date           <= ft.drug_exposure_start_date AND hm.unit_concept_id IN (8840);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c2097aa4-00bf-4010-b1e1-fe5007826c86"),
    Final_Table_8=Input(rid="ri.foundry.main.dataset.e0311527-06c7-48c7-b4cb-42aaa33ee5d0"),
    autoimmune_expressions=Input(rid="ri.foundry.main.dataset.aa1b808d-097d-4256-b838-41ec2e00275c")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    autoimmune_expressions AS p
JOIN    Final_Table_8   AS a
        ON p.person_id = a.person_id
WHERE   p.condition_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.vector.main.execute.17350494-d1a1-43ff-9699-0812bc0f768e"),
    add_long_covid_flag=Input(rid="ri.vector.main.execute.80de645d-9b5e-444c-8662-325ca021f566"),
    remove_null=Input(rid="ri.vector.main.execute.e433f90a-fe10-4784-a45c-efdc93b85bd7")
)
SELECT  r.*
FROM    remove_null            AS r
JOIN    add_long_covid_flag    AS a   ON r.person_id = a.person_id
WHERE   r.measurement_date <= a.drug_exposure_start_date;

@transform_pandas(
    Output(rid="ri.vector.main.execute.4a801443-5a5e-4188-9d1e-a2b9d3ea526f"),
    Final_Table_3=Input(rid="ri.foundry.main.dataset.c5fb653a-2add-4c4f-9f67-36245ec57bab"),
    furosemide_exposures=Input(rid="ri.foundry.main.dataset.219f2691-9a75-447d-bb1b-5bdd4c91115f")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    furosemide_exposures AS p
JOIN    Final_Table_3   AS a
        ON p.person_id = a.person_id
WHERE   p.drug_exposure_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e62b9885-d9b4-4739-a9fa-bea61c17214e"),
    add_furosemide=Input(rid="ri.foundry.main.dataset.50b4d888-feac-4204-be51-eada2fe41d07"),
    glomerular_measures=Input(rid="ri.vector.main.execute.a1d034f6-bd38-4a0b-8114-133ea7658d25")
)
SELECT hm.*
FROM   glomerular_measures  AS hm
JOIN   add_furosemide   AS ft
       ON  hm.person_id = ft.person_id
WHERE  hm.measurement_date           <= ft.drug_exposure_start_date AND hm.unit_concept_id IN (9117, 720870);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0e1abe14-6d34-4c93-b84c-9a62a6405f57"),
    add_t2dm=Input(rid="ri.foundry.main.dataset.125153de-b163-4ea6-addc-393a96dc1219"),
    hba1c_measures=Input(rid="ri.foundry.main.dataset.caba1e53-4e15-4460-99ae-a6f14c1d0397")
)
SELECT hm.*
FROM   hba1c_measures  AS hm
JOIN   add_t2dm   AS ft
       ON  hm.person_id = ft.person_id
WHERE  hm.measurement_date           <= ft.COVID_first_poslab_or_diagnosis_date;

@transform_pandas(
    Output(rid="ri.vector.main.execute.0e78a0b4-c9dc-487b-a4a6-0eb9899e9aed"),
    Final_Table_1=Input(rid="ri.foundry.main.dataset.ad0c3e96-af8e-4b5d-8721-581870bed197"),
    hba1c_measures_1=Input(rid="ri.foundry.main.dataset.11523ac0-f464-4ac8-a1f0-57381fc2e20d")
)
SELECT hm.*
FROM   hba1c_measures_1  AS hm
JOIN   Final_Table_1   AS ft
       ON  hm.person_id = ft.person_id
WHERE  ft.drug_exposure_start_date IS NOT NULL
  AND  hm.measurement_date           <= ft.drug_exposure_start_date;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b212d687-80a1-4a34-8684-33ea73922ac6"),
    Final_Table_8=Input(rid="ri.foundry.main.dataset.e0311527-06c7-48c7-b4cb-42aaa33ee5d0"),
    immune_suppression_expressions=Input(rid="ri.foundry.main.dataset.98439c3b-8283-4a0a-9319-0b3aab1b63e0")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    immune_suppression_expressions AS p
JOIN    Final_Table_8   AS a
        ON p.person_id = a.person_id
WHERE   p.condition_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.vector.main.execute.1b0e7ede-65fe-4b22-a121-5f9966349cf5"),
    Final_Table_3=Input(rid="ri.foundry.main.dataset.c5fb653a-2add-4c4f-9f67-36245ec57bab"),
    torsemide_exposures=Input(rid="ri.foundry.main.dataset.597729fe-b2f0-43cb-80e4-12aa6faa8c0d")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    torsemide_exposures AS p
JOIN    Final_Table_3   AS a
        ON p.person_id = a.person_id
WHERE   p.drug_exposure_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1e2f0090-dcc6-42e3-8279-55f20302b296"),
    drug_exposure_1=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM   drug_exposure_1
WHERE  drug_concept_id IN (
    1356059, 43160969, 19078554, 40988009, 46233876, 46233977, 40052093,
    46234049, 46221557, 42902743, 1560168, 35602729, 40052773, 43295949,
    1596975, 46233973, 44095745, 41112977, 1146285, 19078555, 37499745,
    1718706, 40051340, 19090221, 44183380, 37497419, 1586370, 40832119,
    41112969, 35201904, 43263380, 1718889, 35602720, 1502910, 42479175,
    44043339, 46234236, 43275143, 35753702, 1586346, 44107424, 40052768,
    36891569, 46234240, 42937038, 1513915, 19078559, 40052072, 43517114,
    42937039, 19078632, 19090229, 41050265, 19058398, 1560004, 46234043,
    19078607, 1567201, 1516980, 40052393, 43263337, 19080091, 1596957,
    44044074, 43515904, 40053116, 1531601, 44031039, 1596976, 41207083,
    40159544, 1718876, 40891263, 19078557, 19071700, 1567232, 1596959,
    43516691, 37499744, 42903270, 41268963, 44164600, 1513849, 43514656,
    36407356, 40159520, 1361675, 1592474, 45777025, 21060891, 40729692,
    40051341, 40053117, 1596974, 35602731, 793223, 44044076, 40052361,
    19094121, 41112967, 42902923, 1516976, 40051348, 40184291, 1592475,
    35749499, 43515475, 42480411, 19135263, 41112968, 40052048, 44067484,
    1146779, 46234041, 1586369, 46234042, 21116051, 42899447, 1502908,
    35156919, 40051316, 45777023, 792695, 46233945, 19135264, 46233976,
    19009383, 42481506, 46234051, 44119087, 44121698, 40159545, 40728300,
    45777024, 1588986, 44121699, 19091621, 46234191, 43268823, 46234238,
    1550023, 41080093, 1356058, 2721332, 46234047, 44083575, 43515070,
    43516692, 46233970, 1516978, 19090187, 44172189, 40822618, 43290527,
    40051331, 43515073, 36249934, 35762030, 40130843, 42937042, 2053727,
    40153110, 1596977, 21155450, 42480279, 40051689, 40956762, 1718604,
    43279683, 40052401, 45776621, 19116446, 1516979, 40729690, 46234242,
    35604829, 1146368, 46233974, 1513876, 1513912, 46221553, 42902371,
    46234048, 43516684, 1596956, 40052077, 40130712, 40052382, 46221559,
    40051364, 40159543, 1513881, 40051363, 40821386, 44175888, 1513913,
    1718599, 19078551, 35142133, 41018983, 46234016, 35605621, 44108597,
    1513877, 46234235, 44164598, 19067324, 36249546, 1567200, 42903168,
    40715396, 35757856, 41236426, 37497423, 1544872, 40955175, 40925517,
    19078530, 42902468, 1596999, 36887606, 42480850, 1146780, 19013951,
    40052044, 40166274, 42544060, 40051333, 19078552, 44175889, 46234053,
    41112983, 792697, 36259239, 36267051, 19012266, 1146781, 40052383,
    793225, 42902742, 46233969, 40956761, 19090249, 40051323, 43514641,
    44069926, 46221581, 1361258, 40052045, 19013926, 1544873, 35201903,
    44095746, 46233948, 42963025, 46234237, 2053725, 1718711, 43515888,
    35602724, 46234233, 19123376, 42544059, 36894419, 40176981, 1361676,
    19062457, 40184293, 35602723, 19078527, 19112791, 44068402, 40052370,
    44064884, 46275452, 35604094, 1544838, 45777022, 40892876, 19078603,
    37499746, 19090247, 40159519, 1596972, 36272155, 35152059, 37498227,
    35605622, 1596973, 41175527, 46234098, 43274353, 35602717, 36403491,
    1513914, 40051342, 46234234, 40159689, 19090244, 19135276, 1596916,
    45774400, 37499740, 2718458, 40729700, 1513916, 41237993, 1513850,
    19135320, 40832116, 43285098, 1516981, 40159521, 43285922, 44124065,
    43285105, 19090180, 35154855, 43290519, 44172191, 40052047, 43285151,
    19078558, 1596960, 1502905, 40832115, 21041281, 40052771, 35605670,
    2721334, 1567234, 43515887, 19012270, 36249657, 40821353, 45776625,
    44068832, 43263333, 40892875, 1567198, 44042855, 41050272, 40052772,
    40729691, 40051382, 42903059, 35605620, 1146373, 45774398, 42949468,
    19131582, 41237984, 2718459, 19135265, 44121695, 42937036, 19090226,
    46234244, 19009384, 42937035, 41142933, 19061670, 1356057
);

-- https://unite.nih.gov/workspace/hubble/objects/ri.phonograph2-objects.main.object.be9ad011-20ba-4df5-9686-9eef33bb4a30

@transform_pandas(
    Output(rid="ri.vector.main.execute.f7fc25e1-4bc3-4727-86ab-4cff63516639"),
    add_pcos=Input(rid="ri.foundry.main.dataset.f4312e33-29af-43b9-bbab-34dfcc45f081"),
    insulin_exposures=Input(rid="ri.foundry.main.dataset.1e2f0090-dcc6-42e3-8279-55f20302b296")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    insulin_exposures AS p
JOIN    add_pcos   AS a
        ON p.person_id = a.person_id
WHERE   p.drug_exposure_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.83270446-9c8a-40ae-b1bd-80ed8a345248"),
    ohe_race_ethnicity=Input(rid="ri.foundry.main.dataset.9de96050-1b5b-4428-b921-f4ba6ca909e3"),
    visit_occurrence=Input(rid="ri.foundry.main.dataset.3f74d43a-d981-4e17-93f0-21c811c57aab")
)
-- Flag people who had ≥ 2 distinct visits in the 12 months after their COVID index date
WITH visit_counts AS (          -- count qualifying visits for each person
    SELECT  o.person_id,
            COUNT(DISTINCT v.visit_occurrence_id) AS cnt
    FROM    ohe_race_ethnicity       AS o        -- provides index date
    JOIN    visit_occurrence         AS v
           ON v.person_id = o.person_id
    WHERE   v.visit_start_date >= o.COVID_first_poslab_or_diagnosis_date      -- on/after index
      AND   v.visit_start_date <=  add_months(o.COVID_first_poslab_or_diagnosis_date, 12)  -- < +12 mo
    GROUP BY o.person_id
)
SELECT  o.*,
        CASE WHEN vc.cnt >= 2 THEN 1 ELSE 0 END AS visit_after_enrollment     -- new column
FROM    ohe_race_ethnicity AS o
LEFT JOIN visit_counts     AS vc
       ON o.person_id = vc.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e7d7a562-6e90-42ea-a004-49f348d0b94b"),
    age_filter=Input(rid="ri.foundry.main.dataset.61e2295f-1241-46d2-9718-31ef2560cc11"),
    kidney_occurrence=Input(rid="ri.foundry.main.dataset.f95d2559-947f-485a-9bc2-7c7e4bd571d4")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    kidney_occurrence AS p
JOIN    age_filter             AS a
        ON p.person_id = a.person_id
WHERE   p.condition_start_date <= a.COVID_first_poslab_or_diagnosis_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f95d2559-947f-485a-9bc2-7c7e4bd571d4"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT *
FROM   condition_occurrence
WHERE  condition_concept_id IN (443597, 443612, 443611);

@transform_pandas(
    Output(rid="ri.vector.main.execute.cfb38111-64b7-4084-abde-233f847d972d"),
    index_albumin=Input(rid="ri.foundry.main.dataset.2eff562a-d0a6-4ef0-bd31-653207e778ed")
)
SELECT person_id,
       measurement_date,
       value_as_number
       /* any other columns you need */
FROM (
    SELECT i.*,
           ROW_NUMBER() OVER (PARTITION BY person_id
                              ORDER BY measurement_date DESC) AS rn
    FROM   index_albumin AS i
    WHERE  value_as_number IS NOT NULL
) AS x
WHERE  rn = 1;

@transform_pandas(
    Output(rid="ri.vector.main.execute.6d120135-6c2c-4cd0-b1e1-027f3dfe2eb3"),
    index_glomerular=Input(rid="ri.foundry.main.dataset.e62b9885-d9b4-4739-a9fa-bea61c17214e")
)
SELECT person_id,
       measurement_date,
       value_as_number
       /* any other columns you need */
FROM (
    SELECT i.*,
           ROW_NUMBER() OVER (PARTITION BY person_id
                              ORDER BY measurement_date DESC) AS rn
    FROM   index_glomerular AS i
    WHERE  value_as_number IS NOT NULL
) AS x
WHERE  rn = 1;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b5109cfd-63b5-4d83-82e1-06105066e39c"),
    index_hba1c=Input(rid="ri.foundry.main.dataset.0e1abe14-6d34-4c93-b84c-9a62a6405f57")
)
SELECT
    person_id,
    CASE
        WHEN SUM(CASE WHEN harmonized_value_as_number >= 6.5 THEN 1 ELSE 0 END) > 0
        THEN 1
        ELSE 0
    END AS hba1c_t2dm
FROM index_hba1c
WHERE harmonized_value_as_number IS NOT NULL      -- ignore missing values
GROUP BY person_id;

@transform_pandas(
    Output(rid="ri.vector.main.execute.dd44d4c5-3b9a-4840-9437-cbaa1f3ac86e"),
    index_hba1c_1=Input(rid="ri.vector.main.execute.0e78a0b4-c9dc-487b-a4a6-0eb9899e9aed")
)
SELECT person_id,
       measurement_date,
       harmonized_value_as_number
       /* any other columns you need */
FROM (
    SELECT i.*,
           ROW_NUMBER() OVER (PARTITION BY person_id
                              ORDER BY measurement_date DESC) AS rn
    FROM   index_hba1c_1 AS i
    WHERE  harmonized_value_as_number IS NOT NULL
) AS x
WHERE  rn = 1;

@transform_pandas(
    Output(rid="ri.vector.main.execute.98800a85-12f4-435a-90d7-9615cbe60a8d"),
    add_aspirin=Input(rid="ri.foundry.main.dataset.4ab5e5ab-1955-48d2-997a-06c297681d0f"),
    long_covid_min_date=Input(rid="ri.vector.main.execute.ffb425b3-0fb0-462e-b950-dd194d9e1e8c")
)
SELECT  l.*, a.COVID_first_poslab_or_diagnosis_date
FROM    long_covid_min_date   AS l
JOIN    add_aspirin   AS a
       ON l.person_id = a.person_id
WHERE   l.long_covid_date BETWEEN
            add_months(a.COVID_first_poslab_or_diagnosis_date, 1)     -- ≥ 1 month after
        AND add_months(a.COVID_first_poslab_or_diagnosis_date, 12);   -- ≤ 12 months after

@transform_pandas(
    Output(rid="ri.vector.main.execute.ffb425b3-0fb0-462e-b950-dd194d9e1e8c"),
    long_covid_occurrence=Input(rid="ri.foundry.main.dataset.81fd3f14-c017-4a1c-b11b-4159a0ff955c")
)
SELECT person_id, min(condition_start_date) as long_covid_date
FROM long_covid_occurrence
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.81fd3f14-c017-4a1c-b11b-4159a0ff955c"),
    condition_occurrence_2=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT *
FROM condition_occurrence_2
where condition_concept_id in (705076, 710706)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fc01cfae-b2ce-44b0-86bc-3eff4006c987"),
    exclude_intersection=Input(rid="ri.foundry.main.dataset.acb4905f-054f-4953-a50f-c3bddd888261")
)
-- simplest: take the greatest (latest) non-null date
SELECT
    a.*,
    greatest(
        dpp4i_drug_exposure_start_date,
        all_glp_1_drug_exposure_start_date
    ) AS drug_exposure_start_date
FROM exclude_intersection AS a;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3574ae4f-e5a1-4d5d-82bd-552376f14c35"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM   drug_exposure
WHERE  LOWER(drug_concept_name) LIKE '%metformin%';   -- works in any SQL dialect

@transform_pandas(
    Output(rid="ri.vector.main.execute.a8b649f9-ceb0-4e65-86c9-c7d34e625e5d"),
    index_covid=Input(rid="ri.vector.main.execute.17350494-d1a1-43ff-9699-0812bc0f768e")
)
-- Keep only the latest measurement per person_id in index_covid
WITH ranked AS (
    SELECT  *,
            ROW_NUMBER() OVER (PARTITION BY person_id
                               ORDER BY measurement_date DESC) AS rn
    FROM    index_covid
)
SELECT  *
FROM    ranked
WHERE   rn = 1;          -- most‑recent measurement_date for each person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9de96050-1b5b-4428-b921-f4ba6ca909e3"),
    ohe_sex=Input(rid="ri.vector.main.execute.a1db2633-9a8f-4ff1-8fe4-903b8b77aaf7")
)
SELECT
*, 
    CASE WHEN race_ethnicity = 'White Non-Hispanic' THEN 1 ELSE 0 END 
        AS race_white_non_hispanic,
    CASE WHEN race_ethnicity = 'Black or African American Non-Hispanic' THEN 1 ELSE 0 END 
        AS race_black_or_african_american_non_hispanic,
    CASE WHEN race_ethnicity = 'Hispanic or Latino Any Race' THEN 1 ELSE 0 END 
        AS race_hispanic_or_latino_any_race,
    CASE WHEN race_ethnicity = 'Unknown' THEN 1 ELSE 0 END 
        AS race_unknown,
    CASE WHEN race_ethnicity = 'Asian Non-Hispanic' THEN 1 ELSE 0 END 
        AS race_asian_non_hispanic,
    CASE WHEN race_ethnicity = 'Other Non-Hispanic' THEN 1 ELSE 0 END 
        AS race_other_non_hispanic,
    CASE WHEN race_ethnicity = 'American Indian or Alaska Native Non-Hispanic' THEN 1 ELSE 0 END 
        AS race_american_indian_or_alaska_native_non_hispanic,
    CASE WHEN race_ethnicity = 'Native Hawaiian or Other Pacific Islander Non-Hispanic' THEN 1 ELSE 0 END 
        AS race_native_hawaiian_or_other_pacific_islander_non_hispanic
    -- Include any other columns you want to keep
    
FROM ohe_sex

@transform_pandas(
    Output(rid="ri.vector.main.execute.a1db2633-9a8f-4ff1-8fe4-903b8b77aaf7"),
    add_death_or_pasc=Input(rid="ri.foundry.main.dataset.69242315-13c6-45c4-ac51-2db1f143fc01")
)
SELECT
*, 
    CASE WHEN sex = 'FEMALE' THEN 1 ELSE 0 END AS sex_female,
    CASE WHEN sex = 'MALE' THEN 1 ELSE 0 END AS sex_male,
    CASE WHEN sex = 'No matching concept' THEN 1 ELSE 0 END AS sex_no_matching_concept, 
    CASE WHEN sex = 'UNKNOWN' THEN 1 ELSE 0 END AS sex_unknown,
    CASE WHEN sex = 'OTHER' THEN 1 ELSE 0 END AS sex_other
FROM add_death_or_pasc

@transform_pandas(
    Output(rid="ri.vector.main.execute.d40a0434-2a36-4576-8c6c-3a86755e89ea"),
    merge_drug_exposure_date=Input(rid="ri.foundry.main.dataset.fc01cfae-b2ce-44b0-86bc-3eff4006c987"),
    pcos_occurrence=Input(rid="ri.foundry.main.dataset.681772d7-5f47-43a9-8855-b3d50733ac01")
)
SELECT  p.*, a.drug_exposure_start_date
FROM    pcos_occurrence AS p
JOIN    merge_drug_exposure_date             AS a
        ON p.person_id = a.person_id
WHERE   p.condition_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.681772d7-5f47-43a9-8855-b3d50733ac01"),
    condition_occurrence_1=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT *
FROM condition_occurrence_1
WHERE condition_concept_id IN (
    3576525,
    3654296,
    836764,
    45533028,
    3523163,
    3654294,
    35206936,
    40443308,
    3654295
);

@transform_pandas(
    Output(rid="ri.vector.main.execute.e433f90a-fe10-4784-a45c-efdc93b85bd7"),
    creatinine_measurements=Input(rid="ri.foundry.main.dataset.6463fed3-9d6b-4343-8219-c5187068e8dc")
)
SELECT *
    FROM   creatinine_measurements
    WHERE  harmonized_value_as_number IS NOT NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6e61fd16-6a76-4af3-8aa6-2857fd389494"),
    age_filter=Input(rid="ri.foundry.main.dataset.61e2295f-1241-46d2-9718-31ef2560cc11"),
    renal_occurrence=Input(rid="ri.foundry.main.dataset.416bf67e-1618-4e16-a23c-e059082eed9d")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    renal_occurrence AS p
JOIN    age_filter             AS a
        ON p.person_id = a.person_id
WHERE   p.condition_start_date <= a.COVID_first_poslab_or_diagnosis_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.416bf67e-1618-4e16-a23c-e059082eed9d"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT *
FROM   condition_occurrence
WHERE  condition_concept_id IN (
    193782,
    43021864,
    45757392,
    35207671,
    45596188,
    601166,
    4128200,
    43020455,
    443919,
    762973,
    45772751,
    44784439,
    45768813,
    439695,
    4125970,
    45769906,
    44782717,
    439694,
    46273164,
    45769904,
    45757393,
    45548653,
    4030520,
    37018886,
    35207674
);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f2162776-3c94-407c-a40c-ea578981cc0f"),
    drug_exposure_1=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM   drug_exposure_1
WHERE  drug_concept_id IN (
    1592180,
    1592085,
    1510813,
    1551860,
    1539403,
    1545958,
    1549686,
    40165636
);

-- https://unite.nih.gov/workspace/hubble/objects/ri.phonograph2-objects.main.object.87525bd3-b54b-495b-ba56-fa868a25e852

@transform_pandas(
    Output(rid="ri.vector.main.execute.40b96995-57df-4be7-9a41-99ea8b24ad6c"),
    add_pcos=Input(rid="ri.foundry.main.dataset.f4312e33-29af-43b9-bbab-34dfcc45f081"),
    statins_exposures=Input(rid="ri.foundry.main.dataset.f2162776-3c94-407c-a40c-ea578981cc0f")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    statins_exposures AS p
JOIN    add_pcos   AS a
        ON p.person_id = a.person_id
WHERE   p.drug_exposure_start_date <= a.drug_exposure_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.08a729b4-5353-4104-b98e-43f0e218907b"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da")
)
SELECT *
FROM   drug_exposure
WHERE  drug_concept_id IN (
    835767, 36958832, 780258, 779729, 779711, 36965264, 780263, 779738,
    36929841, 779734, 780266, 36927976, 780262, 779721, 779717, 36923653,
    780264, 597626, 597215, 779723, 779731, 36958884, 36965415, 36933452,
    780256, 36932549, 779728, 36300982, 780259, 779730, 631401, 36930446,
    36948600, 779733, 36937264, 780265, 779710, 780260, 779732, 779718,
    36938127, 36963555, 779725, 36942095, 780257, 779740, 36934548, 36971624,
    779719, 779735, 36953940, 779727, 779737, 779722, 780255, 36939754,
    779726, 779720, 36953121, 779724, 36944132, 36929947, 779739, 779705,
    36964513, 779714, 779736, 780261,
    -- additional GLP-1 agonists
    793143,       -- semaglutide
    45774435,     -- dulaglutide
    1583722,      -- exenatide
    40170911,     -- liraglutide
    44506754      -- lixisenatide
);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.13b7ccdc-8c52-4d56-b92d-c666eb372c38"),
    su_indexed=Input(rid="ri.foundry.main.dataset.0efb3d6b-3e7b-430b-99bb-fa5d0be41773")
)
SELECT  *
FROM   (
        SELECT  m.*,
                ROW_NUMBER() OVER (PARTITION BY person_id
                                   ORDER BY drug_exposure_start_date DESC) AS rn
        FROM    su_indexed AS m
       ) t
WHERE  rn = 1;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0efb3d6b-3e7b-430b-99bb-fa5d0be41773"),
    exclude_diseases=Input(rid="ri.foundry.main.dataset.6ff8e885-c657-46b2-9dbb-28a807927677"),
    su_administrations=Input(rid="ri.foundry.main.dataset.08a729b4-5353-4104-b98e-43f0e218907b")
)
SELECT ma.*, ed.COVID_first_poslab_or_diagnosis_date
FROM   su_administrations AS ma
JOIN   exclude_diseases          AS ed
       ON ma.person_id = ed.person_id
WHERE  ma.drug_exposure_start_date <= date_sub(ed.COVID_first_poslab_or_diagnosis_date, 30)
  AND (ma.drug_exposure_end_date IS NULL
       OR ma.drug_exposure_end_date >= ed.COVID_first_poslab_or_diagnosis_date);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d225c401-c669-45b5-b873-488690b138d9"),
    age_filter=Input(rid="ri.foundry.main.dataset.61e2295f-1241-46d2-9718-31ef2560cc11"),
    t2dm_occurrence=Input(rid="ri.foundry.main.dataset.e12d8531-d4b4-4631-9626-edbd4beac1aa")
)
SELECT  p.*, a.COVID_first_poslab_or_diagnosis_date
FROM    t2dm_occurrence AS p
JOIN    age_filter             AS a
        ON p.person_id = a.person_id
WHERE   p.condition_start_date <= a.COVID_first_poslab_or_diagnosis_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e12d8531-d4b4-4631-9626-edbd4beac1aa"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT *
FROM   condition_occurrence
WHERE  condition_concept_id IN (
    45552386, 40483315, 40480000, 1567968, 37200208, 3346930, 1409152, 37200206,
    45595799, 37200252, 4129379, 1567963, 37200207, 45595797, 35626763, 376065,
    45757392, 45581355, 45552385, 4236285, 43021968, 37200253, 1567971, 45605402,
    4008576, 4029440, 37200211, 1567962, 3198118, 45547625, 45557113, 4171246,
    321822, 45757393, 42539022, 4201636, 37200251, 4171406, 30968, 4034961,
    45591030, 4055679, 4034966, 44833365, 37200240, 45581354, 1567966, 37200229,
    45605401, 4129515, 1326491, 37200246, 37200222, 37200234, 1567961, 37200205,
    37200224, 40769338, 37200219, 3193274, 45533023, 3181307, 37200201, 4221532,
    443767, 4164730, 37200213, 37200202, 45766963, 4060085, 1567969, 37200228,
    45600642, 37200210, 1567967, 36713275, 4216968, 37200203, 35206882, 44807267,
    37200215, 37200199, 1409154, 4082346, 44793113, 443732, 45586139, 40482801,
    37200238, 3087518, 4061725, 37200223, 4159742, 4307799, 43531011, 37200227,
    37200235, 37200209, 4082360, 3182725, 45533022, 37312019, 45605405, 43022019,
    45533021, 1567959, 760977, 45595798, 37200247, 45566731, 45547627, 37200250,
    46270562, 201820, 37200244, 37200212, 1567964, 37200249, 45581352, 37200239,
    4030064, 37200231, 4034963, 761049, 42538715, 1409194, 4220821, 45591027,
    443238, 35206881, 1567960, 37200220, 45591029, 44810261, 4265337, 37200214,
    1409151, 4114426, 1409193, 45537961, 45547626, 4226354, 44809658, 3469133,
    4130165, 45877606, 37396524, 1567965, 45561949, 37110068, 45576443, 45591031,
    37200245, 4226238, 1567956, 37200221, 4297627, 37200243, 45542738, 37200232,
    765375, 201826, 45605403, 1326492, 37200236, 35206880, 37200242, 45533020,
    4019513, 45533019, 37200198, 761050, 760989, 37200233, 4166381, 37200216,
    1326493, 761063, 37200225, 36685758, 761051, 45600641, 442793, 45586140,
    45757508, 45757077, 45605404, 43021173, 37200204, 45537962, 37200230,
    42536400, 40485020, 1567957, 45557112, 760979, 37200241, 37200218, 43531642,
    37200254, 37200217, 37200248, 4043348, 1409150, 1567958, 192279, 1567970,
    443730, 4030066, 37200226, 45581353, 37200237, 4196141, 37200200
);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.037995a6-26e5-4512-a924-69cdb3e972bd"),
    visit_occurrence_1=Input(rid="ri.foundry.main.dataset.3f74d43a-d981-4e17-93f0-21c811c57aab")
)
SELECT *
FROM   visit_occurrence_1
WHERE  person_id       = 2532905483521515525
  AND  visit_start_date < DATE '2021-04-05'; 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.14923780-b8b9-4ce0-b3c7-857f2370c3e8"),
    all_patients_fact_day_table_LDS=Input(rid="ri.foundry.main.dataset.bb008308-b8d0-41b7-b112-49840247d31e")
)
SELECT *
FROM all_patients_fact_day_table_LDS
WHERE person_id = 1029008643044745291 AND date <= '2020-03-29'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1576523b-0aac-4fbb-a3dc-a521b4e18102"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members
WHERE codeset_id=174644251 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.597729fe-b2f0-43cb-80e4-12aa6faa8c0d"),
    drug_exposure_2=Input(rid="ri.foundry.main.dataset.fd499c1d-4b37-4cda-b94f-b7bf70a014da"),
    torsemide_concepts=Input(rid="ri.foundry.main.dataset.1576523b-0aac-4fbb-a3dc-a521b4e18102")
)
SELECT codeset_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_concept_name
FROM drug_exposure_2 INNER JOIN torsemide_concepts
ON drug_exposure_2.drug_concept_id = torsemide_concepts.concept_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.561115d7-3f5e-49d8-94b0-daa4ab9cfcf8"),
    visit_occurrence_1=Input(rid="ri.foundry.main.dataset.3f74d43a-d981-4e17-93f0-21c811c57aab")
)
SELECT *
FROM visit_occurrence_1

