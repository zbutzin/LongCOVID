library(tmle)
library(dplyr)
library(xgboost)

@transform_pandas(
    Output(rid="ri.vector.main.execute.4d303368-e286-4b5d-b1de-8ba91a8befae"),
    PCOS_patients=Input(rid="ri.foundry.main.dataset.51d8ce59-c124-4929-bc4e-429d4540c3fc")
)
PCOS_model <- function(PCOS_patients) {
    df <- PCOS_patients
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('metformin', 'LL_Long_COVID_diagnosis_indicator', 'visit_after_enrollment', 'PREDIABETESRF_indicator', 'PCOS_indicator', 'death_within_period')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)
    # "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart"
    SL.library = c("SL.glm", "SL.glmnet", "SL.xgboost" ) 
    r <- tmle(Y = df[['LL_Long_COVID_diagnosis_indicator']],
              A = df[['metformin']],
              W = df %>% dplyr::select(all_of(cov_names)),
              Delta = df[['visit_after_enrollment']],
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              g.Delta.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))
    # # print EY1, EY0
    # EY1 <- mean(r$Qstar[, "Q1W"])
    # EY0 <- mean(r$Qstar[, 'Q0W'])
    # cat('EY1: ', EY1, '\n')
    # cat('EY0: ', EY0, '\n')
    # result_df = data.frame(matrix(ncol = 15, nrow = 0))
    # result_df = organize_result(df, 'SSRI_Indicator', r, result_df)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.838b1e84-f175-4ef9-a0e0-333a087021f2"),
    prediabetes=Input(rid="ri.foundry.main.dataset.78af18e9-6d94-422e-b680-35360dce7ba4")
)
prediabetes_model <- function(prediabetes) {
    df <- prediabetes
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('metformin', 'LL_Long_COVID_diagnosis_indicator', 'visit_after_enrollment', 'PREDIABETESRF_indicator', 'PCOS_indicator', 'death_within_period')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)
    # "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart"
    SL.library = c("SL.glm", "SL.glmnet", "SL.xgboost") 
    r <- tmle(Y = df[['LL_Long_COVID_diagnosis_indicator']],
              A = df[['metformin']],
              W = df %>% dplyr::select(all_of(cov_names)),
              Delta = df[['visit_after_enrollment']],
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              g.Delta.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))
    # # print EY1, EY0
    # EY1 <- mean(r$Qstar[, "Q1W"])
    # EY0 <- mean(r$Qstar[, 'Q0W'])
    # cat('EY1: ', EY1, '\n')
    # cat('EY0: ', EY0, '\n')
    # result_df = data.frame(matrix(ncol = 15, nrow = 0))
    # result_df = organize_result(df, 'SSRI_Indicator', r, result_df)
    return(NULL)
}

