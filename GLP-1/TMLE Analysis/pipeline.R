library(tmle)
library(dplyr)
library(xgboost)

@transform_pandas(
    Output(rid="ri.vector.main.execute.3b0e4e01-6a9c-43ee-afa7-3ddf99dd6708"),
    Preprocess=Input(rid="ri.foundry.main.dataset.f4b43eb2-1875-448f-8e33-a269cd18616f")
)
death <- function(Preprocess) {
    df <- Preprocess
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('all_glp_1', 'long_covid_in_study_period', 'visit_after_enrollment', 'death_within_12_months', 'death_or_long_covid', 'long_covid_phenotype')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)
    # "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart"
    SL.library = c("SL.glm", "SL.glmnet", "SL.xgboost" ) 
    r <- tmle(Y = df[['death_within_12_months']],
              A = df[['all_glp_1']],
              W = df %>% dplyr::select(all_of(cov_names)),
              Delta = df[['visit_after_enrollment']],
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              g.Delta.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))

        ## summary stats here
        ## ---------- UNADJUSTED SECTION (base-R only) ----------------------
    # 2 × 2 table: rows = treatment (0/1), cols = outcome (0/1)
    tab <- with(df, table(all_glp_1, death_within_12_months))

    a <- tab["1","1"]     # events in treated
    b <- tab["1","0"]
    c <- tab["0","1"]     # events in controls
    d <- tab["0","0"]

    unadj_risk_t  <- a / (a + b)
    unadj_risk_c  <- c / (c + d)
    unadj_rr      <- unadj_risk_t / unadj_risk_c

    # Wald 95 % CI on log-scale, then exponentiate
    logRR         <- log(unadj_rr)
    var_logRR     <- 1/a - 1/(a+b) + 1/c - 1/(c+d)
    se_logRR      <- sqrt(var_logRR)
    z97           <- qnorm(0.975)

    unadj_ci_low  <- exp(logRR - z97 * se_logRR)
    unadj_ci_high <- exp(logRR + z97 * se_logRR)

    ## ---------- ADJUSTED SECTION (still from TMLE) --------------------
    adj_risk_t    <- mean(r$Qstar[, "Q1W"])
    adj_risk_c    <- mean(r$Qstar[, 'Q0W'])
    adj_rr        <- r$estimates$RR
    adj_ci_low    <- r$estimates$CI.RR[1]
    adj_ci_high   <- r$estimates$CI.RR[2]

    ## ---------- PRINT ONE TAB-SEPARATED LINE --------------------------
    out <- c(unadj_risk_t, unadj_risk_c, unadj_rr,
             unadj_ci_low, unadj_ci_high,
             adj_risk_t,   adj_risk_c,   adj_rr,
             adj_ci_low,   adj_ci_high)

    print(out)
    g_df <- as.data.frame(r$g$g1W)
    # result <- c(outcome, r$estimates$RR$psi, r$estimates$RR$CI[1], r$estimates$RR$CI[2], r$estimates$RR$pvalue)
    # df_results <- rbind(df_results, result)
    # # result_df = organize_result(df, 'SSRI_Indicator', r, result_df)
    # colnames(df_results) <- c('Symptom', 'RR', 'RR_CI_lower', 'RR_CI_upper', 'p_value')
    return(g_df) 
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.5c7444a4-82f5-4ee1-9c8d-bfc7ada52ab8"),
    Preprocess=Input(rid="ri.foundry.main.dataset.f4b43eb2-1875-448f-8e33-a269cd18616f")
)
long_covid <- function(Preprocess) {
    df <- Preprocess
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('all_glp_1', 'long_covid_in_study_period', 'visit_after_enrollment', 'death_within_12_months', 'death_or_long_covid', 'long_covid_phenotype')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)
    # "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart"
    SL.library = c("SL.glm", "SL.glmnet", "SL.xgboost" ) 
    # SL.library = c("SL.glm") 
    r <- tmle(Y = df[['long_covid_in_study_period']],
              A = df[['all_glp_1']],
              W = df %>% dplyr::select(all_of(cov_names)),
              Delta = (df[['visit_after_enrollment']]==1) & (df[['death_within_12_months']]==0),
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              g.Delta.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))

        ## summary stats here
        ## ---------- UNADJUSTED SECTION (base-R only) ----------------------
    # 2 × 2 table: rows = treatment (0/1), cols = outcome (0/1)
    tab <- with(df, table(all_glp_1, long_covid_in_study_period))

    a <- tab["1","1"]     # events in treated
    b <- tab["1","0"]
    c <- tab["0","1"]     # events in controls
    d <- tab["0","0"]

    unadj_risk_t  <- a / (a + b)
    unadj_risk_c  <- c / (c + d)
    unadj_rr      <- unadj_risk_t / unadj_risk_c

    # Wald 95 % CI on log-scale, then exponentiate
    logRR         <- log(unadj_rr)
    var_logRR     <- 1/a - 1/(a+b) + 1/c - 1/(c+d)
    se_logRR      <- sqrt(var_logRR)
    z97           <- qnorm(0.975)

    unadj_ci_low  <- exp(logRR - z97 * se_logRR)
    unadj_ci_high <- exp(logRR + z97 * se_logRR)

    ## ---------- ADJUSTED SECTION (still from TMLE) --------------------
    adj_risk_t    <- mean(r$Qstar[, "Q1W"])
    adj_risk_c    <- mean(r$Qstar[, 'Q0W'])
    adj_rr        <- r$estimates$RR
    adj_ci_low    <- r$estimates$CI.RR[1]
    adj_ci_high   <- r$estimates$CI.RR[2]

    ## ---------- PRINT ONE TAB-SEPARATED LINE --------------------------
    out <- c(unadj_risk_t, unadj_risk_c, unadj_rr,
             unadj_ci_low, unadj_ci_high,
             adj_risk_t,   adj_risk_c,   adj_rr,
             adj_ci_low,   adj_ci_high)

    print(out)

    # # print EY1, EY0
    # EY1 <- mean(r$Qstar[, "Q1W"])
    # EY0 <- mean(r$Qstar[, 'Q0W'])
    # cat('EY1: ', EY1, '\n')
    # cat('EY0: ', EY0, '\n')
    # result_df = data.frame(matrix(ncol = 15, nrow = 0))
    # result_df = organize_result(df, 'SSRI_Indicator', r, result_df)
    g_df <- as.data.frame(r$g$g1W)
    # result <- c(outcome, r$estimates$RR$psi, r$estimates$RR$CI[1], r$estimates$RR$CI[2], r$estimates$RR$pvalue)
    # df_results <- rbind(df_results, result)
    # # result_df = organize_result(df, 'SSRI_Indicator', r, result_df)
    # colnames(df_results) <- c('Symptom', 'RR', 'RR_CI_lower', 'RR_CI_upper', 'p_value')
    return(g_df) 
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.e3f76dbd-bf60-4cf3-a928-abcd4e13747f")
)
long_covid_or_death <- function() {
    df <- Preprocess
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('semaglutide', 'long_covid_in_study_period', 'visit_after_enrollment', 'death_within_12_months', 'death_or_long_covid')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)
    # "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart"
    SL.library = c("SL.glm", "SL.glmnet", "SL.xgboost" ) 
    r <- tmle(Y = df[['death_or_long_covid']],
              A = df[['semaglutide']],
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
    Output(rid="ri.vector.main.execute.37161301-90d3-410e-bd34-65204609af7b"),
    Preprocess=Input(rid="ri.foundry.main.dataset.f4b43eb2-1875-448f-8e33-a269cd18616f")
)
long_covid_phenotype <- function(Preprocess) {
    df <- Preprocess
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('all_glp_1', 'long_covid_in_study_period', 'visit_after_enrollment', 'death_within_12_months', 'death_or_long_covid', 'long_covid_phenotype')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)
    # "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart"
    SL.library = c("SL.glm", "SL.glmnet", "SL.xgboost" ) 
    # SL.library = c("SL.glm") 
    r <- tmle(Y = df[['long_covid_phenotype']],
              A = df[['all_glp_1']],
              W = df %>% dplyr::select(all_of(cov_names)),
              Delta = (df[['visit_after_enrollment']]==1) & (df[['death_within_12_months']]==0),
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              g.Delta.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))

        ## summary stats here
        ## ---------- UNADJUSTED SECTION (base-R only) ----------------------
    # 2 × 2 table: rows = treatment (0/1), cols = outcome (0/1)
    tab <- with(df, table(all_glp_1, long_covid_phenotype))

    a <- tab["1","1"]     # events in treated
    b <- tab["1","0"]
    c <- tab["0","1"]     # events in controls
    d <- tab["0","0"]

    unadj_risk_t  <- a / (a + b)
    unadj_risk_c  <- c / (c + d)
    unadj_rr      <- unadj_risk_t / unadj_risk_c

    # Wald 95 % CI on log-scale, then exponentiate
    logRR         <- log(unadj_rr)
    var_logRR     <- 1/a - 1/(a+b) + 1/c - 1/(c+d)
    se_logRR      <- sqrt(var_logRR)
    z97           <- qnorm(0.975)

    unadj_ci_low  <- exp(logRR - z97 * se_logRR)
    unadj_ci_high <- exp(logRR + z97 * se_logRR)

    ## ---------- ADJUSTED SECTION (still from TMLE) --------------------
    adj_risk_t    <- mean(r$Qstar[, "Q1W"])
    adj_risk_c    <- mean(r$Qstar[, 'Q0W'])
    adj_rr        <- r$estimates$RR
    adj_ci_low    <- r$estimates$CI.RR[1]
    adj_ci_high   <- r$estimates$CI.RR[2]

    ## ---------- PRINT ONE TAB-SEPARATED LINE --------------------------
    out <- c(unadj_risk_t, unadj_risk_c, unadj_rr,
             unadj_ci_low, unadj_ci_high,
             adj_risk_t,   adj_risk_c,   adj_rr,
             adj_ci_low,   adj_ci_high)

    print(out)

    # # print EY1, EY0
    # EY1 <- mean(r$Qstar[, "Q1W"])
    # EY0 <- mean(r$Qstar[, 'Q0W'])
    # cat('EY1: ', EY1, '\n')
    # cat('EY0: ', EY0, '\n')
    # result_df = data.frame(matrix(ncol = 15, nrow = 0))
    # result_df = organize_result(df, 'SSRI_Indicator', r, result_df)
    g_df <- as.data.frame(r$g$g1W)
    # result <- c(outcome, r$estimates$RR$psi, r$estimates$RR$CI[1], r$estimates$RR$CI[2], r$estimates$RR$pvalue)
    # df_results <- rbind(df_results, result)
    # # result_df = organize_result(df, 'SSRI_Indicator', r, result_df)
    # colnames(df_results) <- c('Symptom', 'RR', 'RR_CI_lower', 'RR_CI_upper', 'p_value')
    return(g_df) 
}

