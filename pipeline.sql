

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b8afeda9-8ab4-4fe7-aa29-379461f4afe2"),
    LongCovidDates=Input(rid="ri.foundry.main.dataset.121c7066-de3e-443d-8d1c-648c03b78fef")
)
SELECT person_id, min(condition_start_date) as long_covid_date
FROM LongCovidDates
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9b806f31-93f3-4cbd-a8c9-d8f9ff5d7ab6"),
    Semi_final=Input(rid="ri.foundry.main.dataset.c4e74e7e-51d3-4981-b3bb-047aef7f81e8")
)
SELECT *
FROM Semi_final
WHERE PCOS_indicator = 1 OR PREDIABETESRF_indicator = 1;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.817258a4-e831-49b2-be2d-1078b061a881"),
    rename_2=Input(rid="ri.foundry.main.dataset.d9fd0d20-392f-4c78-91dd-b93afeaedca5"),
    rename_covid=Input(rid="ri.foundry.main.dataset.75b7b90a-aa84-4385-bef8-5a2184a82ea2")
)
SELECT ft.*, 
       rc.*
FROM rename_2 as ft
JOIN rename_covid as rc
    ON ft.new_person_id = rc.covid_date_person_id; 

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
    Output(rid="ri.vector.main.execute.05e8be07-8d3f-444b-b13b-91f7a64f9f77"),
    rename_3=Input(rid="ri.foundry.main.dataset.9d1249a8-3d78-452e-b0f4-aa2ee61b149d")
)
SELECT *
FROM rename_3

