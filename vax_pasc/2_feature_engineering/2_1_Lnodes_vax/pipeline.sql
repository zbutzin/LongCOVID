

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.09ac5fe6-b996-4682-b1b7-0f0725b4e5b6"),
    visit_person=Input(rid="ri.foundry.main.dataset.3210d68e-5ff9-4aca-a32c-0c2976de225a")
)
SELECT person_id, (vax_to_visit+1) as vax_to_visit
FROM visit_person vp
WHERE vax_to_visit >= 0
GROUP BY person_id, vax_to_visit

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3210d68e-5ff9-4aca-a32c-0c2976de225a"),
    final_AY=Input(rid="ri.foundry.main.dataset.e70da526-f9f6-4020-aae0-96d99f24814a"),
    visit_occurrence=Input(rid="ri.foundry.main.dataset.911d0bb2-c56e-46bd-af4f-8d9611183bb7")
)
SELECT df.person_id, df.t0, visit.visit_start_date, floor((months_between(visit.visit_start_date, df.t0)-1)/2) as vax_to_visit
FROM final_AY df
LEFT JOIN visit_occurrence visit
ON visit.person_id = df.person_id
WHERE visit.visit_start_date > df.t0
GROUP BY df.person_id, df.t0, visit.visit_start_date

