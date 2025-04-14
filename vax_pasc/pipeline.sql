

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6a150518-42ce-498a-9d3f-7073df5cb7b7"),
    ISC_CommonFacts_COVID_Patient_Summary_Table=Input(rid="ri.foundry.main.dataset.1ae12697-3b58-40e6-87f8-6847a417d755")
)
SELECT *
FROM ISC_CommonFacts_COVID_Patient_Summary_Table

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0f5ab051-4ffd-4913-91c8-0105239399d9"),
    Vaccine_fact_lds=Input(rid="ri.foundry.main.dataset.7482e426-55a2-4a0b-9976-4cb3aa35788d")
)
SELECT *
FROM Vaccine_fact_lds

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b6e90ff2-cc12-4443-8824-b9fce92d5cb4"),
    covid_vax=Input(rid="ri.foundry.main.dataset.07492a72-9134-4de7-a1be-718590fc8542"),
    date_df=Input(rid="ri.foundry.main.dataset.1031d315-de12-4777-ae7f-95a31943e079")
)
-- combine the long covid date with the cohort
-- filter: long_covid date > index date
SELECT cv.*, d.long_covid_date
FROM covid_vax cv
LEFT JOIN date_df d
ON cv.person_id = d.person_id
WHERE (d.long_covid_date > date_add(cv.covid_index, 28)) OR isnull(d.long_covid_date) -- long covid > covid + 28

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.07492a72-9134-4de7-a1be-718590fc8542"),
    ISC_CommonFacts_COVID_Patient_Summary_Table_v163=Input(rid="ri.foundry.main.dataset.6a150518-42ce-498a-9d3f-7073df5cb7b7"),
    vaccine_fact_v163=Input(rid="ri.foundry.main.dataset.bde4997c-cd55-4e74-b44d-34589b2115eb")
)
-- filter the patients based on 
-- data partner AND
-- vaccine date between the window

SELECT s.person_id, s.COVID_first_poslab_or_diagnosis_date as covid_index, s.Long_COVID_diagnosis_post_covid_indicator as long_covid, v.1_vax_date, v.1_vax_type, v.2_vax_date, v.2_vax_type, v.3_vax_date, v.3_vax_type, v.4_vax_date, v.4_vax_type, v.5_vax_date, v.5_vax_type, s.data_partner_id, COUNT(*) OVER (PARTITION BY s.data_partner_id) as total
FROM ISC_CommonFacts_COVID_Patient_Summary_Table_v163 s LEFT JOIN 
vaccine_fact_v163 v
ON s.person_id = v.person_id
WHERE s.data_partner_id IN (23,124,198,399,411,439,507,524,526,726,793)
    -- (SELECT data_partner_id FROM prop_data_partner WHERE vax_prop > 0.05 AND longC_prop > 0.01) -- from selected sites
AND
(v.1_vax_date IS NOT NULL AND v.1_vax_date >= '2021-12-25' AND v.1_vax_date <= '2022-09-25'
OR (v.2_vax_date IS NOT NULL AND v.2_vax_date >= '2021-12-25' AND v.2_vax_date <= '2022-09-25')
OR (v.3_vax_date IS NOT NULL AND v.3_vax_date >= '2021-12-25' AND v.3_vax_date <= '2022-09-25')
OR (v.4_vax_date IS NOT NULL AND v.4_vax_date >= '2021-12-25' AND v.4_vax_date <= '2022-09-25')
OR (v.5_vax_date IS NOT NULL AND v.5_vax_date >= '2021-12-25' AND v.5_vax_date <= '2022-09-25'))
    -- Filter by participants who received some COVID-19 vaccination or booster dose between  December 18, 2021 and December 31, 2022

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.75eaa084-7202-4924-a111-b614691a1a62"),
    ISC_CommonFacts_COVID_Patient_Summary_Table_v163=Input(rid="ri.foundry.main.dataset.6a150518-42ce-498a-9d3f-7073df5cb7b7"),
    vaccine_fact_v163=Input(rid="ri.foundry.main.dataset.bde4997c-cd55-4e74-b44d-34589b2115eb")
)
SELECT s.person_id, s.Long_COVID_diagnosis_post_covid_indicator as long_covid, v.1_vax_date, v.2_vax_date, v.3_vax_date, v.4_vax_date, v.5_vax_date, s.data_partner_id, COUNT(*) OVER (PARTITION BY s.data_partner_id) as total
FROM ISC_CommonFacts_COVID_Patient_Summary_Table_v145 s
LEFT JOIN
vaccine_fact_v145 v
on s.person_id = v.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1031d315-de12-4777-ae7f-95a31943e079"),
    long_covid_condition=Input(rid="ri.vector.main.execute.0f19f069-93c2-4987-b5ce-e92b39b8f998")
)
-- get the long covid date

SELECT max(person_id) as person_id, min(condition_start_date) as long_covid_date
FROM long_covid_condition
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.0f19f069-93c2-4987-b5ce-e92b39b8f998"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT *
FROM condition_occurrence
where condition_concept_id in (705076, 710706)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.518e381b-ae52-4f92-a521-b2ce20a08b67"),
    covid_vax_wo_filter=Input(rid="ri.foundry.main.dataset.75eaa084-7202-4924-a111-b614691a1a62")
)
-- Get the vaccine and long covid proportion from each data partner

with t1 as 
(SELECT data_partner_id, count(*) as qualified_count, total, count(*) / total as vax_prop
FROM covid_vax_wo_filter
WHERE 1_vax_date IS NOT NULL -- one dose of vaccine
GROUP BY data_partner_id, total),
t2 as
(SELECT data_partner_id, count(*) as qualified_count, total, count(*) / total as longC_prop
FROM covid_vax_wo_filter
WHERE long_covid = 1 -- long covid
GROUP BY data_partner_id, total)

SELECT coalesce(t1.data_partner_id, t2.data_partner_id) as data_partner_id, coalesce(t1.total, t2.total) as total, 
    coalesce(vax_prop, 0) as vax_prop, coalesce(longC_prop, 0) as longC_prop
FROM t1
FULL JOIN t2
ON t1.data_partner_id = t2.data_partner_id and t1.total = t2.total

@transform_pandas(
    Output(rid="ri.vector.main.execute.03f2c1a3-9ef6-4675-8d1b-3d3b4d217cc1"),
    vax_prop_site=Input(rid="ri.foundry.main.dataset.916d24cb-7e24-4929-8f8d-4a985b8eb319")
)
SELECT *
FROM vax_prop_site
WHERE percent_overall_any_vaccination_for_sorting > 30
-- (SELECT percentile_approx(percent_overall_any_vaccination_for_sorting, 0.5) - stddev(percent_overall_any_vaccination_for_sorting)
--     FROM vax_prop_site)

@transform_pandas(
    Output(rid="ri.vector.main.execute.2812b844-8801-484e-ac63-6d50a2d96889"),
    vax_prop_site=Input(rid="ri.foundry.main.dataset.916d24cb-7e24-4929-8f8d-4a985b8eb319")
)
SELECT percentile_approx(percent_overall_any_vaccination_for_sorting, 0.5) - stddev(percent_overall_any_vaccination_for_sorting)
FROM vax_prop_site

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bde4997c-cd55-4e74-b44d-34589b2115eb"),
    vaccine_fact=Input(rid="ri.foundry.main.dataset.6cb41007-dac4-4297-bdee-f0268f66adb6")
)
SELECT *
FROM vaccine_fact

