library(tmle)
library(dplyr)
library(foreach)
library(data.table)

est_tmle <- function(df, W_nodes) {
    # include all the covariates
    gform <- "A~"
    Qform <- "Y~"
    W_nodes <-  W_nodes[!W_nodes %in% c('SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator')]
    cov_terms <- paste0(W_nodes, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(Qform)

    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    Qform <- paste(Qform, 'A', sep='+')
    fit <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
            A = df[['SSRI_Indicator']],
            W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, SSRI_Indicator)),
            gform = gform,
            Qform = Qform,
            Q.SL.library = SL.library,
            g.SL.library = SL.library,
            family = 'binomial')

    return(fit)
}

t.test.sample <- function(mu1, mu2, se1, se2, n1, n2){
    SE <- sqrt(se1^2/n1 + se2^2/n2)
    t <- (mu1 - mu2)/SE
    pvalue <- 2*pt(-abs(t), n1+n2-2)
    return(c(t, pvalue))
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.a999c60d-da2d-47ef-b531-93c67eff325c"),
    drug_type_cleaned=Input(rid="ri.foundry.main.dataset.aa9a922b-6cc2-48b5-a538-3d56029bd395")
)
debug_for_fluvoxamine <- function(drug_type_cleaned) {
    df <- drug_type_cleaned
    exposure <- 'vilazodone_indicator'
    drug_types <- c('fluoxetine_indicator', 'sertraline_indicator', 'paroxetine_indicator', 'fluvoxamine_indicator', 'citalopram_indicator', 'vilazodone_indicator', 'escitalopram_indicator')

    df <- df %>% filter(.[[exposure]] == 1 | SSRI_Indicator == 0)
    df_W <- df %>% select(-drug_types) %>% select(-c('SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator'))
    gform <- "A~"
    Qform <- "Y~"
    cov_names <- colnames(df_W)
    for(i in 1:length(cov_names)){
        x <- cov_names[i]
        if (i==1){
            gform <- paste0(gform, x)
            Qform <- paste0(Qform, x)
        }else{
            gform <- paste(gform, x, sep='+')
            Qform <- paste(Qform, x, sep='+')
        }
        
    }
    Qform <- paste(Qform, 'A', sep='+')

    print(nrow(df))
    Q <- glm(Long_COVID_diagnosis_post_covid_indicator ~ fluvoxamine_indicator + ., df, family = "binomial")
    cQ <- coef(Q)
    cQ[is.na(cQ)] <- 0
    print("Coef for A:")
    print(cQ[[exposure]])

    r <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
        A = df[[exposure]],
        W = df_W,
        gform = gform,
        Qform = Qform,
        family = 'binomial',
        cvQinit = FALSE,
        prescreenW.g = TRUE
        )

    print(summary(r)) 
    print("Qinit:")
    print(colMeans(r$Qinit$Q))
    print("Qstar:")
    print(colMeans(r$Qstar))
    print("g1W:")
    print(quantile(r$g$g1W))
    print("gbound:")
    print(r$gbound)
    print("epsilon:")
    print(r$epsilon)

    # debug fluctuation model
    n <- nrow(df)
    A <- df$fluvoxamine_indicator
    Y <- df$Long_COVID_diagnosis_post_covid_indicator
    gi <- r$g$g1W
    Qi <- r$Qinit$Q
    Q0 <- Qi[,1]
    Q1 <- Qi[,2]

    QA <- ifelse(A==0, Q0, Q1)
    g1 <- pmax(gi, 5/sqrt(n)/log(n))
    g0 <- pmax(1-gi, 5/sqrt(n)/log(n))

    #QA <- plogis(QA)
    wt <- (A/g1+(1-A)/g0)
    
    H0 <- (1-A)
    H1 <- (A)
    IC0 <- (1-A)/g0*(Y-Q0)+Q0-mean(Q0)
    IC1 <- A/g1*(Y-Q1)+Q1-mean(Q1)
    D0 <- abs(mean(IC0))
    D1 <- abs(mean(IC1))
    norm <- sqrt(D0^2+D1^2)
    wt2 <- (A/g1)*D1/norm + ((1-A)/g0)*D0/norm
    f <- glm(Y~H0+H1-1, family=binomial(), offset = qlogis(QA), weight = wt2)
    coef(f)
    f_cc <- glm(Y~I(H0/g0)+I(H1/g1)-1, family=binomial(), offset = qlogis(QA))
    coef(f_cc)
    dt <- data.table(A, wt, wt2)
    print(dt[,as.list(quantile(wt)),by=list(A)])
    print(dt[,as.list(quantile(wt2)),by=list(A)])
    print(coef(f))

    #Q0star <- plogis(coef(f)[1]+qlogis(Q0))
    #Q1star <- plogis(coef(f)[2]+qlogis(Q1))
    #QAstar <- plogis((coef(f)[1] * H0 + 
    #                  coef(f)[2] * H1)+qlogis(QA))
    Q0star <- plogis(coef(f_cc)[1]*(1/g0)+qlogis(Q0))
    Q1star <- plogis(coef(f_cc)[2]*(1/g1)+qlogis(Q1))
    QAstar <- plogis((coef(f_cc)[1] * H0 + 
                      coef(f_cc)[2] * H1)+qlogis(QA))
    mean(Q0star)
    mean(Q1star)
    colMeans(r$Qstar)

    IC0 <- (1-A)/g0*(Y-Q0star)+Q0-mean(Q0)
    IC1 <- A/g1*(Y-Q1star)+Q1-mean(Q1)
    IC_ATE <- (A/g1-(1-A)/g0)*(Y-QAstar)+Q1star-Q0star-(mean(Q1star)-mean(Q0star))
    print(mean(IC0))
    print(mean(IC1))

    r_cc <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
        A = df[[exposure]],
        W = df_W,
        gform = gform,
        Qform = Qform,
        family = 'binomial',
        cvQinit = FALSE,
        prescreenW.g = TRUE,
        target.gwt = FALSE
        )

    print(summary(r_cc))

}

@transform_pandas(
    Output(rid="ri.vector.main.execute.84d1b98a-f450-426b-a994-2dcee98097f7"),
    dosage_low=Input(rid="ri.foundry.main.dataset.16a1526b-7318-40f1-a0ef-0af19e22822d")
)
# tmle_R (b04c2f21-2d8e-4970-8a67-bd6adb19abbf): v14
dosage_low_model <- function(dosage_low) {
    df <- dosage_low
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('dosage_60_low_ind', 'Long_COVID_diagnosis_post_covid_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    r <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
              A = df[['dosage_60_low_ind']],
              W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, dosage_60_low_ind)),
              gform = gform,
              Qform = Qform,
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))
    # print EY1, EY0
    EY1 <- mean(r$Qstar[, "Q1W"])
    EY0 <- mean(r$Qstar[, 'Q0W'])
    cat('EY1: ', EY1, '\n')
    cat('EY0: ', EY0, '\n')
}

#################################################
## Global imports and functions included below ##
#################################################

library(tmle)
library(dplyr)

@transform_pandas(
    Output(rid="ri.vector.main.execute.bda95184-d8e0-4086-86e9-60bb7d2a61d7"),
    dosage_none=Input(rid="ri.foundry.main.dataset.5dbb8c35-ca13-4706-a4cf-06bb970ebff4")
)
# tmle_R (b04c2f21-2d8e-4970-8a67-bd6adb19abbf): v14
dosage_model <- function(dosage_none) {
    df <- dosage_none
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('dosage_60_ind', 'Long_COVID_diagnosis_post_covid_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    r <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
              A = df[['dosage_60_ind']],
              W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, dosage_60_ind)),
              gform = gform,
              Qform = Qform,
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))
    # print EY1, EY0
    EY1 <- mean(r$Qstar[, "Q1W"])
    EY0 <- mean(r$Qstar[, 'Q0W'])
    cat('EY1: ', EY1, '\n')
    cat('EY0: ', EY0, '\n')
}

#################################################
## Global imports and functions included below ##
#################################################

library(tmle)
library(dplyr)

@transform_pandas(
    Output(rid="ri.vector.main.execute.a0a3ae12-bb7c-4ba2-8eaa-5b827b1866d7"),
    drug_type_cleaned=Input(rid="ri.foundry.main.dataset.aa9a922b-6cc2-48b5-a538-3d56029bd395")
)
model_drug_type <- function(drug_type_cleaned) {
    df <- drug_type_cleaned
    drug_types <- c('fluoxetine_indicator', 'sertraline_indicator', 'paroxetine_indicator', 'fluvoxamine_indicator', 'citalopram_indicator', 'escitalopram_indicator', 'vilazodone_indicator')
    df_W <- df %>% select(-drug_types) %>% select(-c("SSRI_Indicator", 'Long_COVID_diagnosis_post_covid_indicator'))
    # include all the covariates
    cov_names <- colnames(df_W)
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    for(exposure in drug_types){
        print(exposure)
        # filter those who have the exposure or no SSRI
        df <- drug_type_cleaned
        df <- df %>% filter(.[[exposure]] == 1 | SSRI_Indicator == 0)
        df_W <- df %>% select(-drug_types) %>% select(-c('SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator'))
        n <- nrow(df)

        r <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
            A = df[[exposure]],
            W = df_W,
            gform = gform,
            Qform = Qform,
            Q.SL.library = SL.library,
            g.SL.library = SL.library,
            family = 'binomial')

        # print(summary(r))
        EY1 <- mean(r$Qstar[, "Q1W"])
        EY0 <- mean(r$Qstar[, 'Q0W'])
        cat('Number of A=1: ', sum(df[exposure]), '\n')
        cat('Number of A=0: ', n-sum(df[exposure]), '\n')
        cat('Number of Y=1 in A=1: ', sum(df[df[exposure]==1, 'Long_COVID_diagnosis_post_covid_indicator']), '\n')
        cat('Number of Y=1 in A=0: ', sum(df[df[exposure]==0, 'Long_COVID_diagnosis_post_covid_indicator']), '\n')
        cat('EY1: ', EY1, '\n')
        cat('EY0: ', EY0, '\n')
        # cat('Estimates: ', r$estimates, '\n')
        ate_est <- paste0(r$estimates$ATE$psi, '(', r$estimates$ATE$CI[1], ', ', r$estimates$ATE$CI[2], ')')
        cat('ATE: ', ate_est, '\n')
        RR_est <- paste0(r$estimates$RR$psi, '(', r$estimates$RR$CI[1], ', ', r$estimates$RR$CI[2], ')')
        cat('RR: ', RR_est, '\n')
    }
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.d7a4968a-08c7-4ad0-ae2f-cd7b8ac3e7aa"),
    clean_negative_exposure=Input(rid="ri.foundry.main.dataset.c702a53b-86af-457c-84f7-d376c4f41aa1")
)
# tmle_R (b04c2f21-2d8e-4970-8a67-bd6adb19abbf): v11
model_negative_exposure <- function(clean_negative_exposure) {
    df <- clean_negative_exposure
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('azithromycin_indicator', 'Long_COVID_diagnosis_post_covid_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    r <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
              A = df[['azithromycin_indicator']],
              W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, azithromycin_indicator)),
              gform = gform,
              Qform = Qform,
              family = 'binomial')
    
    print(summary(r))
    # print EY1, EY0
    EY1 <- mean(r$Qstar[, "Q1W"])
    EY0 <- mean(r$Qstar[, 'Q0W'])
    cat('EY1: ', EY1, '\n')
    cat('EY0: ', EY0, '\n')
}

#################################################
## Global imports and functions included below ##
#################################################

library(tmle)
library(dplyr)

@transform_pandas(
    Output(rid="ri.vector.main.execute.9a9592db-2fbe-4177-8581-50b660d3d6b6"),
    clean_negative_outcome=Input(rid="ri.vector.main.execute.5905c8d9-9ada-4299-b6cb-d3224e2099b6")
)
# tmle_R (b04c2f21-2d8e-4970-8a67-bd6adb19abbf): v14
model_negative_outcome <- function(clean_negative_outcome) {
    df <- clean_negative_outcome
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('SSRI_Indicator', 'fracture_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    r <- tmle(Y = df[['fracture_indicator']],
              A = df[['SSRI_Indicator']],
              W = df %>% dplyr::select(-c(fracture_indicator, SSRI_Indicator)),
              gform = gform,
              Qform = Qform,
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))
    # print EY1, EY0
    EY1 <- mean(r$Qstar[, "Q1W"])
    EY0 <- mean(r$Qstar[, 'Q0W'])
    cat('EY1: ', EY1, '\n')
    cat('EY0: ', EY0, '\n')
}

#################################################
## Global imports and functions included below ##
#################################################

library(tmle)
library(dplyr)

@transform_pandas(
    Output(rid="ri.vector.main.execute.dd7a6166-f315-4bf0-8aba-0ee16deecce8"),
    clean_and_impute=Input(rid="ri.foundry.main.dataset.2fad38c1-ae0b-496a-98a7-4abc4296f18a")
)
# tmle_R (b04c2f21-2d8e-4970-8a67-bd6adb19abbf): v14
model_pooled <- function(clean_and_impute) {
    df <- clean_and_impute
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    r <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
              A = df[['SSRI_Indicator']],
              W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, SSRI_Indicator)),
              gform = gform,
              Qform = Qform,
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              family = 'binomial')
    
    print(summary(r))
    # print EY1, EY0
    EY1 <- mean(r$Qstar[, "Q1W"])
    EY0 <- mean(r$Qstar[, 'Q0W'])
    cat('EY1: ', EY1, '\n')
    cat('EY0: ', EY0, '\n')
}

#################################################
## Global imports and functions included below ##
#################################################

library(tmle)
library(dplyr)

@transform_pandas(
    Output(rid="ri.vector.main.execute.bf6be856-7cec-4588-ba59-9aed95d272e5"),
    clean_and_impute=Input(rid="ri.foundry.main.dataset.2fad38c1-ae0b-496a-98a7-4abc4296f18a")
)
# tmle_R (b04c2f21-2d8e-4970-8a67-bd6adb19abbf): v13
test <- function(clean_and_impute) {
    df <- clean_and_impute
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    r <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
              A = df[['SSRI_Indicator']],
              W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, SSRI_Indicator)),
              gform = gform,
              Qform = Qform,
              family = 'binomial')
    
    print(summary(r))
    # print EY1, EY0
    EY1 <- mean(r$Qstar[, "Q1W"])
    EY0 <- mean(r$Qstar[, 'Q0W'])
    cat('EY1: ', EY1, '\n')
    cat('EY0: ', EY0, '\n')
}

#################################################
## Global imports and functions included below ##
#################################################

library(tmle)
library(dplyr)

@transform_pandas(
    Output(rid="ri.vector.main.execute.2a37dc17-a2e8-4e75-8d89-54eefed84ae5"),
    drug_type_cleaned=Input(rid="ri.foundry.main.dataset.aa9a922b-6cc2-48b5-a538-3d56029bd395")
)
test_fluoxetine <- function(drug_type_cleaned) {
    df <- drug_type_cleaned
    exposure <- 'fluoxetine_indicator'
    drug_types <- c('fluoxetine_indicator', 'sertraline_indicator', 'paroxetine_indicator', 'fluvoxamine_indicator', 'citalopram_indicator', 'vilazodone_indicator', 'escitalopram_indicator')

    df <- df %>% filter(.[[exposure]] == 1 | SSRI_Indicator == 0)
    df_W <- df %>% select(-drug_types) %>% select(-c('SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator'))
    gform <- "A~"
    Qform <- "Y~"
    cov_names <- colnames(df_W)
    for(i in 1:length(cov_names)){
        x <- cov_names[i]
        if (i==1){
            gform <- paste0(gform, x)
            Qform <- paste0(Qform, x)
        }else{
            gform <- paste(gform, x, sep='+')
            Qform <- paste(Qform, x, sep='+')
        }
        
    }
    Qform <- paste(Qform, 'A', sep='+')

    r <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
        A = df[[exposure]],
        W = df_W,
        gform = gform,
        Qform = Qform,
        family = 'binomial',
        cvQinit = FALSE,
        prescreenW.g = TRUE
        )

    print(summary(r)) 

    r_cc <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
        A = df[[exposure]],
        W = df_W,
        gform = gform,
        Qform = Qform,
        family = 'binomial',
        cvQinit = FALSE,
        prescreenW.g = TRUE,
        target.gwt = FALSE
        )

    print(summary(r_cc))

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.317e8eec-d2ba-438b-af11-8a0778c4d805"),
    clean_and_impute=Input(rid="ri.foundry.main.dataset.2fad38c1-ae0b-496a-98a7-4abc4296f18a")
)
vim <- function(clean_and_impute) {
    df <- clean_and_impute
    n <- nrow(df)
    df_W <- df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, SSRI_Indicator))
    W_nodes <- colnames(df_W)
    print("Start Fitting")
    vim_fit <- est_tmle(df, W_nodes)
    RR <- vim_fit$estimates$RR
    print('Running full model')
    full_estimates <- c("FULL", RR$psi, RR$CI, RR$pvalue, 0, NULL, 1)
    full_psi <- RR$psi
    full_se <- abs(RR$CI[1] - RR$psi)/1.96
    print(full_estimates)

    all_estimates <- foreach::foreach(vim_W = W_nodes) %do% {
        W_nodes_subset <- setdiff(W_nodes, vim_W)
        vim_fit <- est_tmle(df, W_nodes_subset)
        RR <- vim_fit$estimates$RR
        se <- abs(RR$CI[1]-RR$psi)/1.96
        diff <- full_psi - RR$psi
        test <- t.test.sample(full_psi, RR$psi, full_se, se, n, n)
        estimates <- c(vim_W, RR$psi, RR$CI, RR$pvalue, diff, test[1], test[2])
        print(estimates)
        estimates
  }
    results <- transpose(as.data.frame(all_estimates))
    rownames(results) <- NULL
    colnames(results) <- c('feature', 'psi', 'lower', 'upper', 'p_value', 'psi_diff', 't_statistics', 'p_value_diff')
    results <- rbind(full_estimates, results)
    results <- lapply(results[, 2:ncol(results)], as.numeric)
    return(results)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2096ca87-1603-4e36-a3ca-e01aa70e39ac"),
    clean_and_impute=Input(rid="ri.foundry.main.dataset.2fad38c1-ae0b-496a-98a7-4abc4296f18a")
)
# vim_set (2a3fa8a6-305e-458d-80bd-e699b8ab5c6d): v7
vim_comorbidities <- function(clean_and_impute) {
    df <- clean_and_impute
    n <- nrow(df)
    df_W <- df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator,SSRI_Indicator))
    W_nodes <- colnames(df_W)
    full_psi <- 0.901018711879392
    full_se <- abs(0.860659442450839 - full_psi)/1.96

    # exclude groups of covariates
    excluded_cov <- c("BMI_max_observed_or_calculated_before_or_day_of_covid","CHRONICLUNGDISEASE_before_or_day_of_covid_indicator","DIABETESUNCOMPLICATED_before_or_day_of_covid_indicator","DIABETESCOMPLICATED_before_or_day_of_covid_indicator","OBESITY_before_or_day_of_covid_indicator","OTHERIMMUNOCOMPROMISED_before_or_day_of_covid_indicator,","TOBACCOSMOKER_before_or_day_of_covid_indicator","SYSTEMICCORTICOSTEROIDS_before_or_day_of_covid_indicator","HYPERTENSION_before_or_day_of_covid_indicator","number_of_COVID_vaccine_doses_before_or_day_of_covid")
    W_nodes_subset <- setdiff(W_nodes, excluded_cov)
    print(length(W_nodes_subset))

    # fit tmle with subset covariates
    vim_fit <- est_tmle(df, W_nodes_subset)
    RR <- vim_fit$estimates$RR
    se <- abs(RR$CI[1]-RR$psi)/1.96
    diff <- full_psi - RR$psi
    test <- t.test.sample(full_psi, RR$psi, full_se, se, n, n)
    estimates <- c('comorbidity', RR$psi, RR$CI, RR$pvalue, diff, test[1], test[2])
    print(estimates)
#   vim_results <- rbind(all_estimates, full_estimates)
    results <- transpose(as.data.frame(estimates))
    rownames(results) <- NULL
    colnames(results) <- c('feature', 'psi', 'lower', 'upper', 'p_value', 'psi_diff', 't_statistics', 'p_value_diff')
    return(results)
}

#################################################
## Global imports and functions included below ##
#################################################

library(tmle)
library(dplyr)
library(foreach)
library(data.table)

est_tmle <- function(df, W_nodes) {
    # include all the covariates
    gform <- "A~"
    Qform <- "Y~"
    for(i in 1:length(W_nodes)){
        x <- W_nodes[i]
        if(x!='SSRI_Indicator' & x!='Long_COVID_diagnosis_post_covid_indicator'){
            if (i==1){
                gform <- paste0(gform, x)
                Qform <- paste0(Qform, x)
            }else{
                gform <- paste(gform, x, sep='+')
                Qform <- paste(Qform, x, sep='+')
            }
        }
    }
    Qform <- paste(Qform, 'A', sep='+')
    fit <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
              A = df[['SSRI_Indicator']],
              W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, SSRI_Indicator)),
              gform = gform,
              Qform = Qform,
              family = 'binomial',
              cvQinit = FALSE)

    return(fit)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f6e938e9-bb0c-4ccf-bf3e-25d5da9108ce"),
    clean_and_impute=Input(rid="ri.foundry.main.dataset.2fad38c1-ae0b-496a-98a7-4abc4296f18a")
)
# vim_set (2a3fa8a6-305e-458d-80bd-e699b8ab5c6d): v7
vim_medical <- function(clean_and_impute) {
    df <- clean_and_impute
    n <- nrow(df)
    df_W <- df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator,SSRI_Indicator))
    W_nodes <- colnames(df_W)
    full_psi <- 0.901018711879392
    full_se <- abs(0.860659442450839 - full_psi)/1.96

    # exclude groups of covariates
    excluded_cov <- c("visits_per_month","post_COVID_visit_indicator","number_of_visits_before_covid")
    W_nodes_subset <- setdiff(W_nodes, excluded_cov)
    print(length(W_nodes_subset))

    # fit tmle with subset covariates
    vim_fit <- est_tmle(df, W_nodes_subset)
    RR <- vim_fit$estimates$RR
    se <- abs(RR$CI[1]-RR$psi)/1.96
    diff <- full_psi - RR$psi
    test <- t.test.sample(full_psi, RR$psi, full_se, se, n, n)
    estimates <- c('medical', RR$psi, RR$CI, RR$pvalue, diff, test[1], test[2])
    print(estimates)
#   vim_results <- rbind(all_estimates, full_estimates)
    results <- transpose(as.data.frame(estimates))
    rownames(results) <- NULL
    colnames(results) <- c('feature', 'psi', 'lower', 'upper', 'p_value', 'psi_diff', 't_statistics', 'p_value_diff')
    return(results)
}

#################################################
## Global imports and functions included below ##
#################################################

library(tmle)
library(dplyr)
library(foreach)
library(data.table)

est_tmle <- function(df, W_nodes) {
    # include all the covariates
    gform <- "A~"
    Qform <- "Y~"
    for(i in 1:length(W_nodes)){
        x <- W_nodes[i]
        if(x!='SSRI_Indicator' & x!='Long_COVID_diagnosis_post_covid_indicator'){
            if (i==1){
                gform <- paste0(gform, x)
                Qform <- paste0(Qform, x)
            }else{
                gform <- paste(gform, x, sep='+')
                Qform <- paste(Qform, x, sep='+')
            }
        }
    }
    Qform <- paste(Qform, 'A', sep='+')
    fit <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
              A = df[['SSRI_Indicator']],
              W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, SSRI_Indicator)),
              gform = gform,
              Qform = Qform,
              family = 'binomial',
              cvQinit = FALSE)

    return(fit)
}

