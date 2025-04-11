

@transform_pandas(
    Output(rid="ri.vector.main.execute.02726d07-1f70-4531-bd1b-0c8165be9d05"),
    long_covid_condition=Input(rid="ri.vector.main.execute.b6c4dd90-2158-4cae-9bd2-0f49dc4e9017")
)
SELECT max(person_id) as person_id, min(condition_era_start_date) as long_covid_date
FROM long_covid_condition
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b5bf6239-7c3e-4b5a-8e3e-b4a7b593bcc4"),
    Prediabetes_PCOS_cohort=Input(rid="ri.foundry.main.dataset.31b36e90-9fb7-4b6d-8b6b-41f1c1f9b3cc"),
    valid=Input(rid="ri.foundry.main.dataset.d9f03ba7-9ae2-4a92-87ba-4bfb9578c35c")
)
SELECT b.concept_set_name, b.condition_start_date, l.*
FROM valid AS l INNER JOIN Prediabetes_PCOS_cohort AS b
ON l.person_id=b.person_id
WHERE b.condition_start_date < l.COVID_first_poslab_or_diagnosis_date AND b.condition_start_date > '2020-10-01' AND b.condition_start_date < '2023-11-15'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.31b36e90-9fb7-4b6d-8b6b-41f1c1f9b3cc"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    diagnosis_cohort=Input(rid="ri.foundry.main.dataset.77981ce9-bc5f-46dd-b456-484cd5a751af")
)
with min_s as (
    SELECT c.person_id, MIN(c.condition_start_date) as condition_start_date
    FROM condition_occurrence c JOIN diagnosis_cohort d 
    ON c.condition_concept_id = d.concept_id
    GROUP BY c.person_id),
    s as (
    SELECT c.person_id, d.concept_set_name, MIN(c.condition_start_date) as condition_start_date
    FROM condition_occurrence c join diagnosis_cohort d
    ON c.condition_concept_id = d.concept_id
    GROUP BY c.person_id, d.concept_set_name)
SELECT s.person_id, MIN(s.concept_set_name) as concept_set_name, MIN(s.condition_start_date) as condition_start_date
FROM min_s, s
WHERE min_s.person_id = s.person_id AND min_s.condition_start_date = s.condition_start_date
GROUP BY s.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.b4ad2735-6a2a-43af-997b-e294e9316769"),
    death=Input(rid="ri.foundry.main.dataset.d8cc2ad4-215e-4b5d-bc80-80ffb3454875")
)
SELECT person_id, max(death_date) as death_date
FROM death
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.412cadc9-e8d0-4252-999a-1ad7e5c3545d"),
    all_dates=Input(rid="ri.foundry.main.dataset.871617e4-7817-4dce-b1f3-921d15eb4ec0")
)
SELECT person_id, pasc_t AS num_days, 1 AS event 
FROM all_dates 
WHERE pasc_t IS NOT NULL
UNION ALL
SELECT person_id, death_t, 2
FROM all_dates
WHERE death_t IS NOT NULL
UNION ALL
SELECT person_id, cens_t, 0
FROM all_dates
WHERE (cens_t IS NOT NULL) and (death_t IS NULL)

@transform_pandas(
    Output(rid="ri.vector.main.execute.523416b3-e35d-4668-aad1-f318f0d5dafc"),
    all_dates=Input(rid="ri.foundry.main.dataset.871617e4-7817-4dce-b1f3-921d15eb4ec0")
)
SELECT person_id, pasc_t AS num_days, 1 AS event 
FROM all_dates 
WHERE pasc_t IS NOT NULL
UNION ALL
SELECT person_id, death_t, 2
FROM all_dates
WHERE (death_t IS NOT NULL) AND (pasc_t IS NULL)
UNION ALL
SELECT person_id, cens_t, 0
FROM all_dates
WHERE (cens_t IS NOT NULL) AND (death_t IS NULL) AND (pasc_t IS NULL)

@transform_pandas(
    Output(rid="ri.vector.main.execute.f9ed4c51-4a41-4ee4-b447-9603f9f501eb"),
    visit_occurrence=Input(rid="ri.foundry.main.dataset.911d0bb2-c56e-46bd-af4f-8d9611183bb7")
)
SELECT person_id, MAX(visit_date) AS last_visit_date
FROM (
    SELECT person_id, 
           CASE 
               WHEN visit_end_date IS NULL THEN visit_start_date 
               ELSE visit_end_date 
           END AS visit_date 
    FROM visit_occurrence
) AS subquery
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.b6c4dd90-2158-4cae-9bd2-0f49dc4e9017"),
    condition_era=Input(rid="ri.foundry.main.dataset.cbe7cbcd-4abb-4213-96de-b588c6bb3ba5")
)
SELECT *
FROM condition_era
where condition_concept_id in (705076, 710706)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d9f03ba7-9ae2-4a92-87ba-4bfb9578c35c"),
    Logic_liaison_covid_19_patient_summary_facts_table_v148=Input(rid="ri.foundry.main.dataset.fceec7b1-f69f-406a-8921-cf2d31be3b86")
)
SELECT *
FROM Logic_liaison_covid_19_patient_summary_facts_table_v148
WHERE COVID_first_poslab_or_diagnosis_date > '2020-01-01'

-- covid after January 1, 2020.

