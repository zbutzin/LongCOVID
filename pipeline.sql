

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6e355892-de04-497a-8a7b-a323d7e56b76"),
    Final_table_2=Input(rid="ri.foundry.main.dataset.e3b48760-d3ac-410b-a780-e00f393ff0f5"),
    all_patients_fact_day_table_LDS=Input(rid="ri.foundry.main.dataset.5c331e73-d93a-4316-922e-82b4d06b1131")
)
SELECT 
    ft.*, 
    ap.*
FROM Final_table_2 AS ft
JOIN all_patients_fact_day_table_LDS AS ap
ON ft.new_person_id = ap.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.8d51362c-5fc3-4076-93de-e069e19877c3"),
    Join_1=Input(rid="ri.foundry.main.dataset.6e355892-de04-497a-8a7b-a323d7e56b76")
)
SELECT *
FROM Join_1

