

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c4e22893-6d13-4824-8f6c-3bf19a1f26b5"),
    Final=Input(rid="ri.foundry.main.dataset.a269ae79-4154-4849-8788-0210fc3fc7e8")
)
SELECT SSRI_Indicator, fluoxetine_indicator, sertraline_indicator, paroxetine_indicator, fluvoxamine_indicator, citalopram_indicator, vilazodone_indicator, escitalopram_indicator, Long_COVID_diagnosis_post_covid_indicator
FROM Final

