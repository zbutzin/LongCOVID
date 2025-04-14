library(dplyr)
library(tmle)
library(xgboost)

@transform_pandas(
    Output(rid="ri.vector.main.execute.75095712-d924-4aac-bb7c-40e7e7a19ef3"),
    categorical_df=Input(rid="ri.foundry.main.dataset.22323bf0-5b79-4c5e-b7ed-9d6d0d653842")
)
categorical_model <- function(categorical_df) {
    df <- categorical_df
    # include all the covariates
    cov_names <- colnames(df)
    outcome_names <- cov_names[startsWith(cov_names, "category_")]

    cov_names <- cov_names[!cov_names %in% c('SSRI', 'two_visits_post_covid', outcome_names)]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)
    # "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart"
    SL.library = c("SL.glm", "SL.xgboost" ) 
    for(outcome in outcome_names[1]){
        r <- tmle(Y = df[[outcome]],
            A = df[['SSRI']],
            W = df %>% dplyr::select(all_of(cov_names)),
            Delta = df[['two_visits_post_covid']],
            Q.SL.library = SL.library,
            g.SL.library = SL.library,
            g.Delta.SL.library = SL.library,
            family = 'binomial')
        print(summary(r))
    }
    # result_df = organize_result(df, 'SSRI_Indicator', r, result_df)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.2c7a1758-8ebd-4d8d-be0f-994fde73b942"),
    categorical_df=Input(rid="ri.foundry.main.dataset.22323bf0-5b79-4c5e-b7ed-9d6d0d653842")
)
unnamed <- function(categorical_df) {
    df <- categorical_df
    # include all the covariates
    cov_names <- colnames(df)
    outcome_names <- cov_names[startsWith(cov_names, "category_")]
    print(outcome_names[1])
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.9b41310b-2fbd-4732-a5f5-e2119100c439"),
    parent_df=Input(rid="ri.foundry.main.dataset.c291d8ef-da77-44f3-99a4-ebb2af407881")
)
unnamed_1 <- function(parent_df) {
    
}

