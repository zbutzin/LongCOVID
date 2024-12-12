

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
    Output(rid="ri.foundry.main.dataset.71aac910-05e4-455a-9303-72d94e5d8be0"),
    Join_1=Input(rid="ri.foundry.main.dataset.6e355892-de04-497a-8a7b-a323d7e56b76")
)
SELECT *
FROM Join_1
WHERE date > drug_exposure_start_date;

-- confirm this logic with Zach, it might be the other way around. 
-- how do i make this more similar to all_patients table with the logic? I need four inputs but idk how to input them like that 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fa71db87-90dd-4dc4-a14c-c3dd8bbe7475"),
    unnamed_1=Input(rid="ri.foundry.main.dataset.71aac910-05e4-455a-9303-72d94e5d8be0")
)
WITH covid_dates AS (
    SELECT DISTINCT
        person_id,
        Date as covid_date
    FROM unnamed_1
    WHERE confirmed_covid_patient = 1
)

SELECT DISTINCT t.*
FROM unnamed_1 t
LEFT JOIN covid_dates c
    ON t.person_id = c.person_id
WHERE 
    -- Include all non-Long Covid rows
    t."LL_Long_COVID_diagnosis" = 0
    OR 
    -- Include Long Covid rows that meet the timing criteria
    (t."LL_Long_COVID_diagnosis" = 1 
     AND EXISTS (
        SELECT 1 
        FROM covid_dates c2 
        WHERE c2.person_id = t.person_id
          AND t.Date >= DATEADD(month, 1, c2.covid_date)  -- At least 1 month after
          AND t.Date <= DATEADD(month, 12, c2.covid_date)  -- At most 12 months after
     ))
ORDER BY t.person_id, t.Date;

