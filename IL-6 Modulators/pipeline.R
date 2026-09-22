# Immune modulators and Long COVID
#
# Analysis code for "IL-6 receptor antagonists and severe post-COVID-19
# outcomes among patients with rheumatoid arthritis", an emulated target
# trial in the National COVID Cohort Collaborative (N3C).
#
# Exposure : treatment_group (1 = tocilizumab or sarilumab,
#                             0 = anakinra or baricitinib)
# Outcomes : long_covid_12m_outcome        (Long COVID diagnosis, ICD-10 U09.9)
#            death_date_within_12mo_inclusion (all-cause mortality)
#            comp_pheno_LC_12_mo_outcome   (probable Long COVID, computational
#                                           phenotype)
# Censoring: at_least_1_visits_after_inclusion (Delta node)
#
# Estimator: single-timepoint TMLE (R package `tmle`) with Super Learner for
# the outcome regression, the treatment mechanism, and the censoring
# mechanism. All three use the library c("SL.glm", "SL.glmnet", "SL.xgboost").
# Every other tmle() argument is left at the package default, so the fits use
# 10-fold cross-validation with cross-fitting of the initial outcome
# regression (V = 10, cvQinit = TRUE) and the data-adaptive propensity
# truncation bound 5/sqrt(n)/log(n).
#
# NOTE ON REPRODUCIBILITY
# This code is provided for transparency and cannot be executed outside the
# N3C Data Enclave. The underlying patient-level data are protected and are
# not distributed with this repository. Access to the data is managed by
# NCATS and requires a data use agreement, a data use request, and N3C
# approval for legacy data access:
# https://ncats.nih.gov/research/research-activities/n3c/resources/data-access
#
# These functions were originally run as transforms in an N3C Enclave
# (Palantir Foundry) code workbook. Each took its analytic table as the
# `change_date_col` input. The Foundry dataset RIDs were not included in the
# export, so the @transform_pandas decorators present in the other analysis
# folders in this repository are omitted here.

library(tmle)
library(dplyr)

# Long COVID (traditional diagnosis, ICD-10 U09.9) --------------------------

treatment_vs_control_new_lc_2 <- function(change_date_col) {

    df <- change_date_col

    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c(
        'treatment_group', 'long_covid_12m_outcome',
        'at_least_1_visits_after_inclusion',
        'death_or_long_covid', 'long_covid_months_since_enrollment',
        'tocilizumab_indicator', 'sarilumab_indicator',
        'anakinra_indicator', 'baricitinib_indicator',
        'hosp_date_within_12mo_inclusion', 'comp_pheno_LC_12_mo_outcome',
        'age_at_covid', 'death_date_within_12mo_inclusion')]

    cov_terms <- paste0(cov_names, collapse = '+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)

    # ---- Unadjusted results ----

    cat("\n===== Unadjusted Outcome Distribution =====\n")
    tab <- table(Treatment = df$treatment_group, LongCOVID = df$long_covid_12m_outcome)
    print(tab)

    prop_table <- prop.table(tab, margin = 1)
    cat("\nProportion with Long COVID by treatment group:\n")
    print(round(prop_table, 3))

    p_treat   <- mean(df$long_covid_12m_outcome[df$treatment_group == 1], na.rm = TRUE)
    p_control <- mean(df$long_covid_12m_outcome[df$treatment_group == 0], na.rm = TRUE)

    rd <- p_treat - p_control
    rr <- p_treat / p_control

    cat("\nUnadjusted Risk in Treated:", round(p_treat, 3), "\n")
    cat("Unadjusted Risk in Control:", round(p_control, 3), "\n")
    cat("Unadjusted Risk Difference (Treated - Control):", round(rd, 3), "\n")
    cat("Unadjusted Risk Ratio (Treated / Control):", round(rr, 3), "\n")

    # ---- Unadjusted CIs ----

    n_treat   <- sum(df$treatment_group == 1)
    n_control <- sum(df$treatment_group == 0)

    # Risk in Treated CI
    se_p_treat       <- sqrt(p_treat * (1 - p_treat) / n_treat)
    p_treat_ci_lower <- p_treat - 1.96 * se_p_treat
    p_treat_ci_upper <- p_treat + 1.96 * se_p_treat
    cat("Unadjusted Risk in Treated 95% CI: (", round(p_treat_ci_lower, 4), ",", round(p_treat_ci_upper, 4), ")\n")

    # Risk in Control CI
    se_p_control       <- sqrt(p_control * (1 - p_control) / n_control)
    p_control_ci_lower <- p_control - 1.96 * se_p_control
    p_control_ci_upper <- p_control + 1.96 * se_p_control
    cat("Unadjusted Risk in Control 95% CI: (", round(p_control_ci_lower, 4), ",", round(p_control_ci_upper, 4), ")\n")

    # Risk Difference CI
    se_rd        <- sqrt((p_treat * (1 - p_treat) / n_treat) + (p_control * (1 - p_control) / n_control))
    rd_ci_lower  <- rd - 1.96 * se_rd
    rd_ci_upper  <- rd + 1.96 * se_rd
    rd_pvalue    <- 2 * (1 - pnorm(abs(rd / se_rd)))
    cat("Unadjusted Risk Difference 95% CI: (", round(rd_ci_lower, 4), ",", round(rd_ci_upper, 4), ")\n")
    cat("Unadjusted Risk Difference p-value:", format.pval(rd_pvalue, digits = 4), "\n")

    # Risk Ratio CI (log scale)
    se_log_rr       <- sqrt((1 - p_treat) / (n_treat * p_treat) + (1 - p_control) / (n_control * p_control))
    log_rr          <- log(rr)
    log_rr_ci_lower <- log_rr - 1.96 * se_log_rr
    log_rr_ci_upper <- log_rr + 1.96 * se_log_rr
    rr_ci_lower     <- exp(log_rr_ci_lower)
    rr_ci_upper     <- exp(log_rr_ci_upper)
    rr_pvalue       <- 2 * (1 - pnorm(abs(log_rr / se_log_rr)))
    cat("Unadjusted Risk Ratio 95% CI: (", round(rr_ci_lower, 4), ",", round(rr_ci_upper, 4), ")\n")
    cat("Unadjusted Risk Ratio p-value:", format.pval(rr_pvalue, digits = 4), "\n\n")

    # ---- Adjusted results (Super Learner + TMLE) ----

    SL.library <- c("SL.glm", "SL.glmnet", "SL.xgboost")

    r <- tmle(Y = df[['long_covid_12m_outcome']],
              A = df[['treatment_group']],
              W = df %>% dplyr::select(all_of(cov_names)),
              Delta = df[['at_least_1_visits_after_inclusion']],
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              g.Delta.SL.library = SL.library,
              family = 'binomial')

    print(summary(r))

    return(NULL)
}

# Death (all-cause mortality) -----------------------------------------------

treatment_vs_control_death <- function(change_date_col) {

    df <- change_date_col

    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c(
        'treatment_group', 'long_covid_12m_outcome',
        'at_least_1_visits_after_inclusion',
        'death_or_long_covid', 'long_covid_months_since_enrollment',
        'tocilizumab_indicator', 'sarilumab_indicator',
        'anakinra_indicator', 'baricitinib_indicator',
        'hosp_date_within_12mo_inclusion', 'comp_pheno_LC_12_mo_outcome',
        'age_at_covid', 'death_date_within_12mo_inclusion')]

    cov_terms <- paste0(cov_names, collapse = '+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)

    # ---- Unadjusted results ----

    cat("\n===== Unadjusted Outcome Distribution =====\n")
    tab <- table(Treatment = df$treatment_group, death = df$death_date_within_12mo_inclusion)
    print(tab)

    prop_table <- prop.table(tab, margin = 1)
    cat("\nProportion with Long COVID by treatment group:\n")
    print(round(prop_table, 3))

    p_treat   <- mean(df$death_date_within_12mo_inclusion[df$treatment_group == 1], na.rm = TRUE)
    p_control <- mean(df$death_date_within_12mo_inclusion[df$treatment_group == 0], na.rm = TRUE)

    rd <- p_treat - p_control
    rr <- p_treat / p_control

    cat("\nUnadjusted Risk in Treated:", round(p_treat, 3), "\n")
    cat("Unadjusted Risk in Control:", round(p_control, 3), "\n")
    cat("Unadjusted Risk Difference (Treated - Control):", round(rd, 3), "\n")
    cat("Unadjusted Risk Ratio (Treated / Control):", round(rr, 3), "\n")

    # ---- Unadjusted CIs ----

    n_treat   <- sum(df$treatment_group == 1)
    n_control <- sum(df$treatment_group == 0)

    # Risk in Treated CI
    se_p_treat       <- sqrt(p_treat * (1 - p_treat) / n_treat)
    p_treat_ci_lower <- p_treat - 1.96 * se_p_treat
    p_treat_ci_upper <- p_treat + 1.96 * se_p_treat
    cat("Unadjusted Risk in Treated 95% CI: (", round(p_treat_ci_lower, 4), ",", round(p_treat_ci_upper, 4), ")\n")

    # Risk in Control CI
    se_p_control       <- sqrt(p_control * (1 - p_control) / n_control)
    p_control_ci_lower <- p_control - 1.96 * se_p_control
    p_control_ci_upper <- p_control + 1.96 * se_p_control
    cat("Unadjusted Risk in Control 95% CI: (", round(p_control_ci_lower, 4), ",", round(p_control_ci_upper, 4), ")\n")

    # Risk Difference CI
    se_rd        <- sqrt((p_treat * (1 - p_treat) / n_treat) + (p_control * (1 - p_control) / n_control))
    rd_ci_lower  <- rd - 1.96 * se_rd
    rd_ci_upper  <- rd + 1.96 * se_rd
    rd_pvalue    <- 2 * (1 - pnorm(abs(rd / se_rd)))
    cat("Unadjusted Risk Difference 95% CI: (", round(rd_ci_lower, 4), ",", round(rd_ci_upper, 4), ")\n")
    cat("Unadjusted Risk Difference p-value:", format.pval(rd_pvalue, digits = 4), "\n")

    # Risk Ratio CI (log scale)
    se_log_rr       <- sqrt((1 - p_treat) / (n_treat * p_treat) + (1 - p_control) / (n_control * p_control))
    log_rr          <- log(rr)
    log_rr_ci_lower <- log_rr - 1.96 * se_log_rr
    log_rr_ci_upper <- log_rr + 1.96 * se_log_rr
    rr_ci_lower     <- exp(log_rr_ci_lower)
    rr_ci_upper     <- exp(log_rr_ci_upper)
    rr_pvalue       <- 2 * (1 - pnorm(abs(log_rr / se_log_rr)))
    cat("Unadjusted Risk Ratio 95% CI: (", round(rr_ci_lower, 4), ",", round(rr_ci_upper, 4), ")\n")
    cat("Unadjusted Risk Ratio p-value:", format.pval(rr_pvalue, digits = 4), "\n\n")

    # ---- Adjusted results (Super Learner + TMLE) ----

    SL.library <- c("SL.glm", "SL.glmnet", "SL.xgboost")

    r <- tmle(Y = df[['death_date_within_12mo_inclusion']],
              A = df[['treatment_group']],
              W = df %>% dplyr::select(all_of(cov_names)),
              Delta = df[['at_least_1_visits_after_inclusion']],
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              g.Delta.SL.library = SL.library,
              family = 'binomial')

    print(summary(r))

    return(NULL)
}

# Long COVID computational phenotype (probable Long COVID) ------------------

Treatment_vs_control_comp_pheno <- function(change_date_col) {

    df <- change_date_col

    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c(
        'treatment_group', 'long_covid_12m_outcome',
        'at_least_1_visits_after_inclusion',
        'death_or_long_covid', 'long_covid_months_since_enrollment',
        'tocilizumab_indicator', 'sarilumab_indicator',
        'baricitinib_indicator', 'anakinra_indicator',
        'hosp_date_within_12mo_inclusion', 'comp_pheno_LC_12_mo_outcome',
        'age_at_covid', 'death_date_within_12mo_inclusion')]

    cov_terms <- paste0(cov_names, collapse = '+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)

    # ---- Unadjusted results ----

    cat("\n===== Unadjusted Outcome Distribution =====\n")
    tab <- table(Treatment = df$treatment_group, comp_long = df$comp_pheno_LC_12_mo_outcome)
    print(tab)

    prop_table <- prop.table(tab, margin = 1)
    cat("\nProportion with Long COVID by treatment group:\n")
    print(round(prop_table, 3))

    p_treat   <- mean(df$comp_pheno_LC_12_mo_outcome[df$treatment_group == 1], na.rm = TRUE)
    p_control <- mean(df$comp_pheno_LC_12_mo_outcome[df$treatment_group == 0], na.rm = TRUE)

    rd <- p_treat - p_control
    rr <- p_treat / p_control

    cat("\nUnadjusted Risk in Treated:", round(p_treat, 3), "\n")
    cat("Unadjusted Risk in Control:", round(p_control, 3), "\n")
    cat("Unadjusted Risk Difference (Treated - Control):", round(rd, 3), "\n")
    cat("Unadjusted Risk Ratio (Treated / Control):", round(rr, 3), "\n")

    # ---- Unadjusted CIs ----

    n_treat   <- sum(df$treatment_group == 1)
    n_control <- sum(df$treatment_group == 0)

    # Risk in Treated CI
    se_p_treat       <- sqrt(p_treat * (1 - p_treat) / n_treat)
    p_treat_ci_lower <- p_treat - 1.96 * se_p_treat
    p_treat_ci_upper <- p_treat + 1.96 * se_p_treat
    cat("Unadjusted Risk in Treated 95% CI: (", round(p_treat_ci_lower, 4), ",", round(p_treat_ci_upper, 4), ")\n")

    # Risk in Control CI
    se_p_control       <- sqrt(p_control * (1 - p_control) / n_control)
    p_control_ci_lower <- p_control - 1.96 * se_p_control
    p_control_ci_upper <- p_control + 1.96 * se_p_control
    cat("Unadjusted Risk in Control 95% CI: (", round(p_control_ci_lower, 4), ",", round(p_control_ci_upper, 4), ")\n")

    # Risk Difference CI
    se_rd        <- sqrt((p_treat * (1 - p_treat) / n_treat) + (p_control * (1 - p_control) / n_control))
    rd_ci_lower  <- rd - 1.96 * se_rd
    rd_ci_upper  <- rd + 1.96 * se_rd
    rd_pvalue    <- 2 * (1 - pnorm(abs(rd / se_rd)))
    cat("Unadjusted Risk Difference 95% CI: (", round(rd_ci_lower, 4), ",", round(rd_ci_upper, 4), ")\n")
    cat("Unadjusted Risk Difference p-value:", format.pval(rd_pvalue, digits = 4), "\n")

    # Risk Ratio CI (log scale)
    se_log_rr       <- sqrt((1 - p_treat) / (n_treat * p_treat) + (1 - p_control) / (n_control * p_control))
    log_rr          <- log(rr)
    log_rr_ci_lower <- log_rr - 1.96 * se_log_rr
    log_rr_ci_upper <- log_rr + 1.96 * se_log_rr
    rr_ci_lower     <- exp(log_rr_ci_lower)
    rr_ci_upper     <- exp(log_rr_ci_upper)
    rr_pvalue       <- 2 * (1 - pnorm(abs(log_rr / se_log_rr)))
    cat("Unadjusted Risk Ratio 95% CI: (", round(rr_ci_lower, 4), ",", round(rr_ci_upper, 4), ")\n")
    cat("Unadjusted Risk Ratio p-value:", format.pval(rr_pvalue, digits = 4), "\n\n")

    # ---- Adjusted results (Super Learner + TMLE) ----

    SL.library <- c("SL.glm", "SL.glmnet", "SL.xgboost")

    r <- tmle(Y = df[['comp_pheno_LC_12_mo_outcome']],
              A = df[['treatment_group']],
              W = df %>% dplyr::select(all_of(cov_names)),
              Delta = df[['at_least_1_visits_after_inclusion']],
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              g.Delta.SL.library = SL.library,
              family = 'binomial')

    print(summary(r))

    return(NULL)
}
