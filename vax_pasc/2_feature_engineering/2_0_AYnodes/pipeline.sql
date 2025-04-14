

@transform_pandas(
    Output(rid="ri.vector.main.execute.b9c6b1f0-0cbe-434a-83d0-9bd42e53a72c"),
    get_t0=Input(rid="ri.vector.main.execute.ad809463-78de-44f2-a451-8b0d6d1b2360"),
    reinfection_wide=Input(rid="ri.vector.main.execute.621f779b-9d53-4de1-bac1-29b6881a8140")
)
SELECT df.person_id, df.covid_index, df.t0, 1_reinfect_date, 2_reinfect_date, 3_reinfect_date, 4_reinfect_date
FROM get_t0 df
LEFT JOIN reinfection_wide re
on df.person_id = re.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.8359711f-3145-4cb8-92c6-ca6fd969fd75"),
    get_covid_date=Input(rid="ri.foundry.main.dataset.cb526597-cbe2-4b89-b829-378b98c3cac8"),
    pre_baseline=Input(rid="ri.vector.main.execute.9ab54747-baaa-4d06-8877-36c821299de1")
)
SELECT covid.treatment_date, baseline.*
FROM get_covid_date covid
JOIN pre_baseline baseline
ON covid.person_id = baseline.person_id
WHERE baseline.long_covid_date > date_add(covid.treatment_date, 28) OR isnull(baseline.long_covid_date)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e18f7c91-fdd5-4639-acfb-c67e636dbec1"),
    Add_date_df=Input(rid="ri.foundry.main.dataset.b6e90ff2-cc12-4443-8824-b9fce92d5cb4"),
    Reinfection_60_days=Input(rid="ri.foundry.main.dataset.9b0f42da-0a91-435a-bb3d-9600ed3573fc")
)
SELECT Reinfection_60_days.*
FROM Add_date_df, Reinfection_60_days
WHERE Add_date_df.person_id = Reinfection_60_days.person_id

