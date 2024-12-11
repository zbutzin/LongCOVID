

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
    Output(rid="ri.foundry.main.dataset.71aac910-05e4-455a-9303-72d94e5d8be0"),
    Join_1=Input(rid="ri.foundry.main.dataset.6e355892-de04-497a-8a7b-a323d7e56b76")
)
SELECT *
FROM Join_1
WHERE date > drug_exposure_start_date;

-- confirm this logic with Zach, it might be the other way around. 
-- how do i make this more similar to all_patients table with the logic? I need four inputs but idk how to input them like that 

