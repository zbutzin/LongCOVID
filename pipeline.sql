

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fa71db87-90dd-4dc4-a14c-c3dd8bbe7475"),
    Join_1=Input(rid="ri.foundry.main.dataset.6e355892-de04-497a-8a7b-a323d7e56b76")
)
SELECT * 
FROM Join_1 
WHERE OBESITY = 0;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6e355892-de04-497a-8a7b-a323d7e56b76"),
    all_patients_fact_day_table_LDS=Input(rid="ri.foundry.main.dataset.5c331e73-d93a-4316-922e-82b4d06b1131"),
    rename_1=Input(rid="ri.foundry.main.dataset.c55947ba-e74b-4027-acb2-0a3bb56908d0")
)
SELECT 
    ft.*, 
    ap.*
FROM rename_1 AS ft
JOIN all_patients_fact_day_table_LDS AS ap
ON ft.new_person_id = ap.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.edb63160-f46d-4fd2-84a4-a1528eabdbd3"),
    all_patients_fact_day_table_LDS=Input(rid="ri.foundry.main.dataset.5c331e73-d93a-4316-922e-82b4d06b1131"),
    rename_2=Input(rid="ri.foundry.main.dataset.d9fd0d20-392f-4c78-91dd-b93afeaedca5")
)
SELECT 
    ft.*, 
    ap.*
FROM rename_2 AS ft
JOIN all_patients_fact_day_table_LDS AS ap
ON ft.new_person_id = ap.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1edb9e65-519a-4ef8-ac66-558c0cd40725"),
    unnamed_1=Input(rid="ri.foundry.main.dataset.71aac910-05e4-455a-9303-72d94e5d8be0")
)
WITH filtered_table AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY new_person_id
            ORDER BY date DESC
        ) AS rn
    FROM unnamed_1
)
SELECT *
FROM filtered_table
WHERE rn = 1;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4acf9e07-8cc0-4c1d-be99-5d925ea7d314"),
    INVALID_TABLE=Input(rid="ri.foundry.main.dataset.fa71db87-90dd-4dc4-a14c-c3dd8bbe7475")
)
SELECT *
FROM unnamed_2
WHERE date > drug_exposure_start_date;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.71aac910-05e4-455a-9303-72d94e5d8be0"),
    Join_1=Input(rid="ri.foundry.main.dataset.6e355892-de04-497a-8a7b-a323d7e56b76")
)
SELECT *
FROM Join_1
WHERE date > drug_exposure_start_date
ORDER BY date DESC;

-- confirm this logic with Zach, it might be the other way around. 
-- how do i make this more similar to all_patients table with the logic? I need four inputs but idk how to input them like that 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0d41e69a-0bb4-4569-8dd0-adb55e1b348d"),
    Join_2=Input(rid="ri.foundry.main.dataset.edb63160-f46d-4fd2-84a4-a1528eabdbd3")
)
WITH filtered_patients AS (
    SELECT 
        *,
        CASE 
            WHEN LL_Long_COVID_diagnosis = 1 
                AND drug_exposure_start_date <= DATEADD(month, 12, drug_exposure_start_date) 
                THEN 1
            ELSE 0
        END as long_covid_status
    FROM Join_2
)
SELECT *
FROM filtered_patients
WHERE long_covid_status IS NOT NULL;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.237eb4db-0dd2-48b7-8e4f-50b38a8acf4b"),
    Join_2=Input(rid="ri.foundry.main.dataset.edb63160-f46d-4fd2-84a4-a1528eabdbd3")
)
WITH filtered_patients AS (
    SELECT
        *,
        CASE
            WHEN LL_Long_COVID_diagnosis = 1
                 AND LL_Long_COVID_diagnosis_date BETWEEN drug_exposure_start_date 
                                                     AND DATEADD(month, 12, drug_exposure_start_date)
            THEN 1
            ELSE 0
        END AS long_covid_status
    FROM Join_2
)
SELECT *
FROM filtered_patients
WHERE long_covid_status = 1;

