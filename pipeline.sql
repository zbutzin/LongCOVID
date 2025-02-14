

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b8afeda9-8ab4-4fe7-aa29-379461f4afe2"),
    LongCovidDates=Input(rid="ri.foundry.main.dataset.121c7066-de3e-443d-8d1c-648c03b78fef")
)
SELECT person_id, min(condition_start_date) as long_covid_date
FROM LongCovidDates
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.edb63160-f46d-4fd2-84a4-a1528eabdbd3"),
    all_patients_fact_day_table_LDS=Input(rid="ri.foundry.main.dataset.5c331e73-d93a-4316-922e-82b4d06b1131"),
    rename_2=Input(rid="ri.foundry.main.dataset.d9fd0d20-392f-4c78-91dd-b93afeaedca5"),
    rename_covid=Input(rid="ri.foundry.main.dataset.75b7b90a-aa84-4385-bef8-5a2184a82ea2")
)
SELECT 
    ft.*, 
    ap.*,
    rc.*
FROM rename_2 AS ft
JOIN all_patients_fact_day_table_LDS AS ap
    ON ft.new_person_id = ap.person_id
JOIN rename_covid AS rc
    ON ft.new_person_id = rc.covid_date_person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.121c7066-de3e-443d-8d1c-648c03b78fef"),
    condition_occurrence_1=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT *
FROM condition_occurrence_1
where condition_concept_id in (705076, 710706)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.94dce3b8-b337-45ae-87f2-40661cb5e181"),
    dedepe=Input(rid="ri.foundry.main.dataset.d7db2751-1f46-4bb2-a5dd-cf0ef3ed4278")
)
SELECT 
    d.*,
    CASE 
        -- Keep as 1 if diagnosis is within 12 months of drug exposure
        WHEN d.long_covid_date BETWEEN d.drug_exposure_start_date 
            AND add_months(d.drug_exposure_start_date, 12) 
            AND d.LL_Long_COVID_diagnosis = 1 THEN 1
        -- Set to 0 in all other cases where it was previously 1
        WHEN d.LL_Long_COVID_diagnosis = 1 THEN 0
        -- Keep existing value for rows that were already 0
        ELSE d.LL_Long_COVID_diagnosis
    END as LL_Long_COVID_diagnosis
FROM dedepe d

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.237eb4db-0dd2-48b7-8e4f-50b38a8acf4b"),
    Join_2=Input(rid="ri.foundry.main.dataset.edb63160-f46d-4fd2-84a4-a1528eabdbd3")
)
WITH filtered_patients AS (
    SELECT
        *,
        CASE
            WHEN LL_Long_COVID_diagnosis = 1
                 AND long_covid_date BETWEEN drug_exposure_start_date 
                                         AND DATEADD(month, 12, drug_exposure_start_date)
            THEN 1
            ELSE 0
        END AS long_covid_status
    FROM Join_2
)
SELECT *
FROM filtered_patients
WHERE long_covid_status = 1 
      AND long_covid_date >= drug_exposure_start_date; 

