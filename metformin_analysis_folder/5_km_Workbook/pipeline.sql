

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.51d8ce59-c124-4929-bc4e-429d4540c3fc"),
    preprocess=Input(rid="ri.foundry.main.dataset.f8e0ad06-7858-42a1-99d3-6b4bf3ee1067")
)
SELECT *
FROM preprocess
WHERE PCOS_indicator=1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.78af18e9-6d94-422e-b680-35360dce7ba4"),
    preprocess=Input(rid="ri.foundry.main.dataset.f8e0ad06-7858-42a1-99d3-6b4bf3ee1067")
)
SELECT *
FROM preprocess
WHERE PREDIABETESRF_indicator=1

