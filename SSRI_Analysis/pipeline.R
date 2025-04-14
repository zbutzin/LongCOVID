library(tmle)
library(SuperLearner)
library(dplyr)

get_tmle_est = function(fit){
  return(c(fit$estimates$RR$log.psi, fit$estimates$RR$var.log.psi))
}
################################################################################
# @Organization - CTML
# @Project - Causal surveillance
# @Description - This file is responsible for estimating the variance of IC of logRR
################################################################################

# Helper functions -------
bound = function(X, alpha=0.0005){
  # maxX <- max(X)
  # minX <- min(X) 
  # f <- maxX - minX
  # X_new = (X-minX)/f
  X[X>=1-alpha] <- 1-alpha
  X[X<=alpha] <- alpha 
  return(X)
}

unbound = function(X){
  maxX <- max(X)
  minX <- min(X) 
  f <- maxX - minX
  return(X*f + minX)
}

calc_stop_crit = function(IC, n){
  thres <- sqrt(var(IC))/(sqrt(n)*log(n))
  return (abs(mean(IC)) <= thres) # stop when this is true
}

#' Calculate the variance from IC
#'
#' Calculate the variance from IC by var = mean(IC^2)/n
#'
#' @param df dataframe
#' @return A numeric vector of variance
#'

calc_var_from_IC = function(df, IC){
  n <- nrow(df)
  v_var <- mean(IC^2)/n
  v_var
}

# Updating functions -------
## Helper functions -------

# return a vector of initial influence curve of logRR
IC_initial_fn = function(Q1, Q0, g1){
  g0 <- 1-g1
  mu1 <- mean(Q1)
  mu0 <- mean(Q0)
  IC_init <- 1/mu1^2 * Q1*(1-Q1)/g1 + 1/mu0^2 * Q0*(1-Q0)/g0 + (Q1/mu1-Q0/mu0)^2
  return(IC_init)
}

# the formula of variance of IC of logRR
var_logRR_fn = function(Q1, Q0, g1){
  IC_initial <- IC_initial_fn(Q1, Q0, g1)
  return(mean(IC_initial))
}

#' A helper function to use the formula to calculate the influence curve of variance of log risk ratio
#'
#' Calculate the influence curve of variance of log risk ratio with updated Q and g
#' 
#' @param A 
#' @param Y
#' @param Q1 estimated Q1 = E[Y|A=1, W]
#' @param Q0 estimated Q0 = E[Y|A=0, W]
#' @param g1 treatment mechanism estimates, P(A = 1|W)
#' @return A numeric vector of the influence curve
#'
IC_var_logRR_fn = function(A, Y, Q1, Q0, g1){
  g0 <- 1-g1
  mu1 <- mean(Q1)
  mu0 <- mean(Q0)
  I1 <- as.numeric(A==1)
  I0 <- as.numeric(A==0)
  
  # clever covariates
  H1 <- clever_covar_Q_fn(Q1, Q0, g1, 'A1')
  H0 <- clever_covar_Q_fn(Q1, Q0, g1, 'A0')
  
  IC_initial <- IC_initial_fn(Q1, Q0, g1)
  
  # influence curves
  D_w <- IC_initial - mean(IC_initial)
  D_y <-  H1 * (I1/g1*(Y-Q1)) + H0 * (I0/g0*(Y-Q0))
  D_g <- -1/mu1^2 * Q1*(1-Q1)/g1^2 * (I1-g1) - 1/mu0^2 * Q0*(1-Q0)/g0^2 * (I0-g0)
  
  return(D_w + D_y + D_g)
}

clever_covar_Q_fn = function(Q1, Q0, g1, cate='A1'){
  mu1 <- mean(Q1)
  mu0 <- mean(Q0)
  g0 <- 1-g1
  theta1 <- mean(Q1*(1-Q1)/g1)
  theta2 <- mean(Q0*(1-Q0)/g0)
  theta3 <- mean(Q1^2)
  theta4 <- mean(Q0^2)
  theta5 <- mean(Q1*Q0)
  
  if(cate=='A1'){
    H <- 1/mu1^2 * ((1-2*Q1)/g1 + 2*Q1 - 2*mu1*Q0/mu0 - 
                      2/mu1*(theta1 + theta3) + 2/mu0* theta5)
  }else{
    H <- 1/mu0^2 * ((1-2*Q0)/g0 + 2*Q0 - 2*mu0*Q1/mu1 - 
                      2/mu0*(theta2 + theta4) + 2/mu1* theta5)
  }
  return(H)
}

clever_covar_g_fn = function(Q1, Q0, g1){
  mu1 <- mean(Q1)
  mu0 <- mean(Q0)
  g0 <- 1-g1
  Hg <- 1/mu0^2 * Q0*(1-Q0) / g0^2 - 
    1/mu1^2 * Q1*(1-Q1) / g1^2
  
  return(Hg)
}

## One-step -------
# TODO: record the number of increments v.s loss - computational cost
#' Calculate the influence curve of variance of log risk ratio
#'
#' Calculate the influence curve of variance of log risk ratio using universal least favorable model: https://rdrr.io/cran/tmle/src/R/tmle.R
#' 
#' @param Q a 3-column matrix (Q(A,W), Q(1,W), Q(0,W))
#' @param g1W treatment mechanism estimates, P(A = 1|W)
#' @param Y
#' @param A
#' @param depsilon=0.001 step size for delta moves, set to 0.001
#' @param max_iter
#' @return A list of psi, Q, g and convergence
#'
one_step_update = function(Q, g1W, Y, A, stop_crit = TRUE, logRR = TRUE, depsilon=0.0001, max_iter = 100){
  n <- length(Y)
  # g_alpha <- 5/sqrt(n)/log(n)
  g_alpha <- 0.05
  I1 <- as.numeric(A==1)
  I0 <- as.numeric(A==0)
  g1W.prev <- g1W <- bound(g1W, alpha = g_alpha)
  Q.prev <- Q <- bound(Q)
  # mu0 <- mu0.prev <- mean(Q[,"Q0W"])
  # mu1 <- mu1.prev <- mean(Q[,"Q1W"])
  
  # define loss function
  calcLoss <- function(Q, g1W){
    -mean(Y * log(Q[,"QAW"]) + (1-Y) * log(1 - Q[,"QAW"]) + A * log(g1W) + (1-A) * log(1 - g1W))
  }
  
  # define parameter of interest: the variance of IC of logRR
  psi.prev <- psi <- var_logRR_fn(Q[,"Q1W"], Q[,"Q0W"], g1W)
  H1 <- clever_covar_Q_fn(Q[,"Q1W"], Q[,"Q0W"], g1W, cate='A1')
  H0 <- clever_covar_Q_fn(Q[,"Q1W"], Q[,"Q0W"], g1W, cate='A0')
  
  HQ.AW <- H1 * (I1/g1W) + H0 * (I0/(1-g1W))
  Hg <- clever_covar_g_fn(Q[,"Q1W"], Q[,"Q0W"], g1W)
  
  # determine the sign of derivative: DQ + Dg - just need the sign
  # Dg <- Hg * (A-g1W)
  # DQ <- HQ.AW * (Y-Q[, "QAW"])
  IC.prev <- IC.cur <- IC_var_logRR_fn(A, Y, Q[,"Q1W"], Q[,"Q0W"], g1W)
  
  # TODO: should I add DW
  IC_initial <- IC_initial_fn(Q[,"Q1W"], Q[,"Q0W"], g1W)
  deriv <-  mean(IC.prev- IC_initial + mean(IC_initial))
  if (deriv > 0) {
    depsilon <- -depsilon
  }
  
  loss.prev <- Inf
  loss.cur <-  calcLoss(Q, g1W)
  if(is.nan(loss.cur) | is.na(loss.cur) | is.infinite(loss.cur)) { 
    loss.cur <- Inf
    loss.prev <- 0
  }
  iter <-  0
  
  # Define continue condition
  if(stop_crit){
    cond <- (!calc_stop_crit(IC.cur, n)) & (iter < max_iter) & (loss.prev > loss.cur)
  }else{
    cond <- loss.prev > loss.cur & iter < max_iter
  }
  ########## changes
  vars <- c(psi.prev)
  bias <- c(mean(IC.prev))
  losses <- c(loss.cur)

 #TODO: plot the mean of influence curve for each iteration
  while (cond){
    IC.prev <- IC.cur
    Q.prev <- Q
    g1W.prev <- g1W	
    
    # update g1W from g1W.prev and Hg
    Hg <- clever_covar_g_fn(Q.prev[,"Q1W"], Q.prev[,"Q0W"], g1W.prev)
    g1W <- bound(plogis(qlogis(g1W.prev) - depsilon  * Hg), g_alpha) 
    
    # update Q from previous Q and HQ
    H1 <- clever_covar_Q_fn(Q.prev[,"Q1W"], Q.prev[,"Q0W"], g1W.prev, cate='A1')
    H0 <- clever_covar_Q_fn(Q.prev[,"Q1W"], Q.prev[,"Q0W"], g1W.prev, cate='A0')
    
    HQ <- cbind(HAW = H1 * (I1/g1W.prev) + H0 * (I0/(1-g1W.prev)),
                H0W = H0 * 1/(1-g1W.prev),
                H1W = H1 * 1/g1W.prev) 
    
    
    Q <- bound(plogis(qlogis(Q.prev) - depsilon * HQ))
    # mu1.prev <- mu1
    # mu0.prev <- mu0
    # mu0 <- mean(Q[,"Q0W"])
    # mu1 <- mean(Q[,"Q1W"])
    
    # update psi
    psi.prev <- psi
    psi <- var_logRR_fn(Q[,"Q1W"], Q[,"Q0W"], g1W)
    loss.prev <- loss.cur

    ######### changes
    vars <- c(vars, psi)
    bias <- c(bias, mean(IC.cur))
    losses <- c(losses, loss.cur)

    
    # update loss
    loss.cur <- calcLoss(Q, g1W)
    # cat('iter: ', iter, '\n')
    # cat('loss: ', loss.cur, '\n')
    
    # update IC 
    IC.cur <- IC_var_logRR_fn(A, Y, Q[,"Q1W"], Q[,"Q0W"], g1W)
    if(is.nan(loss.cur) | is.infinite(loss.cur) | is.na(loss.cur)) {loss.cur <- Inf}
    iter <- iter + 1
    depsilon <- depsilon * 0.5

    if(stop_crit){
      cond <- (!calc_stop_crit(IC.cur, n)) & (iter < max_iter) & (loss.prev > loss.cur)
    }else{
      cond <- loss.prev > loss.cur & iter < max_iter
    }
    if (iter == max_iter) {
        warning("Max number of iteration reached, stop TMLE")
    }
  }
  # plot the iteration v.s variance
  cat('iteration: ', iter, '\n')
  hist(IC.cur)
  
  plot(vars/n, type = "l", main = "One-step", xlab = "Index", ylab = "one-step")
  plot(bias, type = "l", main = "Bias", xlab = "Index", ylab = "Bias")
  plot(losses, type='l', main='Losses', xlab = "Index", ylab = "Losses")

  
  return(list(psi = psi.prev, Q = Q.prev, g1n = g1W.prev, conv = loss.prev < loss.cur))
}

## Iterative -------

iterative_update = function(Q, ginit, Y, A, stop_crit=TRUE, logRR=TRUE, max_iter=1000){
  n <- length(Y)
  g_alpha <- 5/sqrt(n)/log(n)
  Q <- bound(Q)
  g1W <- bound(ginit$g1W, alpha = g_alpha)
  if(logRR){
    g0W <- bound(ginit$g0W, alpha = g_alpha)
  }
  
  I1 <- as.numeric(A==1)
  I0 <- as.numeric(A==0)
  IC.prev <- IC.cur <- IC_var_logRR_fn(A, Y, Q[,"Q1W"], Q[,"Q0W"], g1W)
  
  thres <- 1e-10
  ep <- epG <- 1
  i <- 0
  # TODO: DQ and Dg separately?
  
  # Define continue condition
  if(stop_crit){
    cond <- (!calc_stop_crit(IC.cur, n)) && (i < max_iter)
  }else{
    cond <- (abs(epG) > thres || abs(ep) > thres) && i < max_iter
  }
  
  while (cond){
    if(logRR){
      IC.prev <- IC.cur
      H1 <- clever_covar_Q_fn(Q[,"Q1W"], Q[,"Q0W"], g1W, cate='A1')
      H0 <- clever_covar_Q_fn(Q[,"Q1W"], Q[,"Q0W"], g1W, cate='A0')
      HQ <- H1 * (I1/g1W) + H0 * (I0/g0W)
      
      # update Q
      # TODO: change it to super learner
      fit <- glm(Y ~ -1 + offset(qlogis(Q[,"QAW"])) + HQ, family = binomial())
      Q[,"QAW"] <- bound(fit$fitted.values) # update offset
      ep <- coef(fit)
      H1Q <- H1 * 1/g1W
      Q[,"Q1W"] <- bound(plogis(qlogis(Q[,"Q1W"]) + ep * H1Q))
      
      H0Q <- H0 * 1/g0W
      Q[,"Q0W"]<- bound(plogis(qlogis(Q[,"Q0W"]) + ep * H0Q))
      
      # update g
      Hg <- clever_covar_g_fn(Q[,"Q1W"], Q[,"Q0W"], g1W)
      epG <- coef(glm(A ~ -1 + offset(qlogis(g1W)) + Hg, family = binomial()))
      g_star <- bound(plogis(qlogis(g1W) + epG * Hg), alpha=g_alpha)
      g1W <- g_star
      g0W <- 1 - g_star
      IC.cur <- IC_var_logRR_fn(A, Y, Q[,"Q1W"], Q[,"Q0W"], g1W)
      
    }else{
      HQ <- as.numeric(A==1)/g1W * ((1-2*Q[,"Q1W"])/g1W + 2*(Q[,"Q1W"]-mean(Q[,"Q1W"])))
      
      # update Q
      ep <- coef(glm(Y ~ -1 + offset(qlogis(Q[,"Q1W"])) + HQ, family = binomial(), subset = A==1))
      HQ <- 1/g1W * ((1-2*Q[,"Q1W"])/g1W + 2*(Q[,"Q1W"]-mean(Q[,"Q1W"])))
      Q[,"Q1W"] <- bound(plogis(qlogis(Q[,"Q1W"]) + ep * HQ))
      
      # update g
      Hg <- Q[,"Q1W"]*(1-Q[,"Q1W"])/g1W^2
      epG <- coef(glm(A ~ -1 + offset(qlogis(g1W)) + Hg, family = binomial()))
      g_star <- bound(plogis(qlogis(g1W) + epG * Hg))
      
      g1W <- g_star
    }
    i <- i + 1
    if(stop_crit){
      cond <- (!calc_stop_crit(IC.cur, n)) && (i < max_iter) 
    }else{
      cond <- (abs(epG) > thres || abs(ep) > thres) && i < max_iter
    }
  }
  # if(i==maxIter){
  #   cat('epG: ', epG)
  #   cat('\n ep: ', ep)
  # }
  if(logRR){
    # cat('Total iteration: ', i, '\n')
    return(list(Q = Q, g1n = g1W))
  }else{
    return(list(Q=Q, g1n = g_star))
  }
}

# Main function -------

#' Estimate variance of logRR with targeted variance
#'
#' estimate the targeted variance of logRR using targeted variance
#' 
#' @param df a dataframe composed of Y, A and W
#' @param tmle_fit a tmle fit of df
#' @param update which update method for the targeted variance, can be 'simple', 'iterative', or 'one-step'
#' @return A list composed of the estimate of the variance and its influence curves
#'

var_robust_logRR = function(df, tmle_fit, update='simple', stop_crit = TRUE){
  n <- nrow(df)
  gbound <- 5/sqrt(n)/log(n)
  # tmle_r <- estimate_tmle(df)
  tmle_r <- tmle_fit
  
  A <- df$A
  Y <- df$Y
  
  Q0W <- tmle_r$Qinit$Q[, 1]
  Q1W <- tmle_r$Qinit$Q[, 2]
  QAW <- as.numeric(A==1)*Q1W + as.numeric(A==0)*Q0W
  g1W <- tmle_r$g$g1W
  g0W <- 1 - g1W
  g <- as.numeric(A==1)*g1W + as.numeric(A==0)*g0W
  
  # Q <- list(QAW = QAW, Q0W = Q0W, Q1W = Q1W)
  Q <- matrix(c(QAW, Q0W, Q1W), nrow = n, ncol = 3, dimnames = list(NULL, c('QAW', 'Q0W', 'Q1W')))
  ginit <- list(g=g, g0W = g0W, g1W = g1W)
  
  
  if(update == 'iterative'){ # use updated Q and g from IC of var
    r = iterative_update(Q, ginit, Y, A, stop_crit)
    Q1_star <- r$Q[, 'Q1W']
    Q0_star <- r$Q[, 'Q0W']
    g1W <- r$g1n
  }else if (update == 'one-step'){
    r = one_step_update(Q, g1W, Y, A, stop_crit)
    Q1_star <- r$Q[, 'Q1W']
    Q0_star <- r$Q[, 'Q0W']
    g1W <- r$g1n
  }else{ # simple plug-in
    Q0_star <- tmle_r$Qinit$Q[, 1]
    Q1_star <- tmle_r$Qinit$Q[, 2]
  }
  
  # TODO: bound Q as well
  # EY0 <- mean(Q0_star)
  # EY1 <- mean(Q1_star)
  g0W <- 1 - g1W

  # bound g1 and g0
  g1W_bdd <- bound(g1W, alpha=gbound)
  # g1W_bdd <- g1W
  # g1W_bdd[g1W_bdd<gbound] = gbound
  # g0W_bdd <- g0W
  # g0W_bdd[g0W_bdd<gbound] = gbound
  hist(Q1_star, main=paste0('Q1W: ', update))
  cat(update, '\n')
  cat('Q1', '\n')
  print(summary(Q1_star))
  cat('Q0', '\n')
  print(summary(Q0_star))
  hist(g1W_bdd, man=paste0('g1W: ', update))
  print(summary(g1W_bdd))
  print(summary(1-g1W_bdd))
  
  v <- var_logRR_fn(Q1_star, Q0_star, g1W_bdd)
  # v <- 1/EY1^2 * mean(Q1_star*(1-Q1_star)/g1W_bdd) +
  #   1/EY0^2 * mean(Q0_star*(1-Q0_star)/g0W_bdd) +
  #   mean((Q1_star/EY1 - Q0_star/EY0)^2)
  
  IC <- IC_var_logRR_fn(A, Y, Q1_star, Q0_star, g1W_bdd)
  
  return(list(est = v/n, IC = IC))
}

#' Calculate the variance estimator of EY1
#' 
#' @param df dataframe
#' @return A numeric vector of influence curve
#'
var_robust_EYa = function(df){
  Y <- df$Y
  A <- df$A
  n = nrow(df)
  tmle_r <- estimate_tmle(df)
  Q0W <- tmle_r$Qinit$Q[, 1]
  Q1W <- tmle_r$Qinit$Q[, 2]
  QAW <- as.numeric(A==1)*Q1W + as.numeric(A==0)*Q0W
  g1W <- tmle_r$g$g1W
  g0W <- 1 - g1W
  g <- as.numeric(A==1)*g1W + as.numeric(A==0)*g0W
  
  
  Q <- matrix(c(QAW, Q0W, Q1W), nrow = n, ncol = 3, dimnames = list(NULL, c('QAW', 'Q0W', 'Q1W')))
  ginit <- list(g=g, g0W = g0W, g1W = g1W)
  
  # update Q and g
  r <- iterative_update(Q, ginit, Y, A, stop_crit=FALSE, logRR=FALSE)
  
  Q <- r$Q[, 'Q1W']
  g <- r$g1n
  
  phi <- mean(Q)
  D_w <- Q*(1-Q)/g - mean(Q*(1-Q)/g) + (Q-phi)^2 - mean((Q-phi)^2)
  D_y <- A/g * ((1-2*Q)/g + 2*(Q-phi)) * (Y-Q)
  D_g <- -Q*(1-Q)/g^2 * (A-g)
  
  est <- mean(Q*(1-Q)/g + (Q-phi)^2)
  return(list(est=est/n, IC=D_w + D_y + D_g))
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c3203fde-9563-4b57-9e96-e1e4b868ef0f"),
    cleaned=Input(rid="ri.foundry.main.dataset.b3adf548-df4b-4b44-aba4-a1e3712c7fb7")
)
sample <- function(cleaned) {
    # post indicator to be 1
    df <- cleaned %>% filter(post_COVID_visit_indicator==1) %>% select(-post_COVID_visit_indicator)
    df <- df %>% sample_n(2000, replace = FALSE)

    return(df)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.dc5f3fc4-e69b-4d1c-88f1-0e2101263bdb"),
    Clean_negative_outcome=Input(rid="ri.foundry.main.dataset.960cfbb6-dbe5-4bc4-a0c0-5cd6163aced3")
)
sample2 <- function(Clean_negative_outcome) {
    df <- Clean_negative_outcome %>% filter(post_COVID_visit_indicator==1) %>% select(-post_COVID_visit_indicator) %>% select(c(number_of_visits_before_covid, SSRI_Indicator, fracture_indicator, BMI_max_observed_or_calculated_before_or_day_of_covid, DIABETESUNCOMPLICATED_before_or_day_of_covid_indicator, TOBACCOSMOKER_before_or_day_of_covid_indicator, sex_FEMALE, race_ethnicity_White_Non_Hispanic, cdm_name_OMOP))
    df <- df %>% sample_n(500, replace = FALSE)
    return(df)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.c64f9124-9ef0-4720-8785-4d2236508823"),
    cleaned=Input(rid="ri.foundry.main.dataset.b3adf548-df4b-4b44-aba4-a1e3712c7fb7")
)
unnamed <- function(cleaned) {
    df <- cleaned
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator', 'post_COVID_visit_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    Dform <- paste('Delta~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    print(Dform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    fit <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
              A = df[['SSRI_Indicator']],
              W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, SSRI_Indicator, post_COVID_visit_indicator)),
              gform = gform,
              Qform = Qform,
            #   Q.SL.library = SL.library,
            #   g.SL.library = SL.library,
              family = 'binomial')
              
    print(summary(fit))
    g1W <- fit$g$g1W
    n <- length(g1W)
    alpha <- 5/sqrt(n)/log(n)
    prop <- sum((g1W < alpha) | (g1W > (1-alpha))) / n
    print(prop)
    tmle_r <- get_tmle_est(fit)
    # change the column name to A and Y
    # df <- df %>% rename(A = SSRI_Indicator, Y = Long_COVID_diagnosis_post_covid_indicator)

    # v_subs <- var_robust_logRR(df, fit)$est
    # v_iter_new <- var_robust_logRR(df, fit, update='iterative')$est
    # v_one_new <- var_robust_logRR(df, fit, update='one-step')$est
        
    # print(c(tmle_r[1], tmle_r[2], v_subs, v_iter_new, v_one_new))
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.b1e108e6-df0e-4347-8eb8-43ef8dfacd3d"),
    sample=Input(rid="ri.foundry.main.dataset.c3203fde-9563-4b57-9e96-e1e4b868ef0f")
)
unnamed_1 <- function(sample) {
    df <- sample
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('SSRI_Indicator', 'Long_COVID_diagnosis_post_covid_indicator', 'post_COVID_visit_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    # print(gform)
    # print(Qform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart")
    suppressWarnings({
        fit <- tmle(Y = df[['Long_COVID_diagnosis_post_covid_indicator']],
            A = df[['SSRI_Indicator']],
            W = df %>% dplyr::select(-c(Long_COVID_diagnosis_post_covid_indicator, SSRI_Indicator)),
            gform = gform,
            Qform = Qform,
            Q.SL.library = SL.library,
            g.SL.library = SL.library,
            family = 'binomial')
    
              
    # print(summary(fit))
    Q1W <- fit$Q
    g1W <- fit$g$g1W
    n <- length(g1W)
    alpha <- 5/sqrt(n)/log(n)
    prop <- sum((g1W < alpha) | (g1W > (1-alpha))) / n
    cat('Proportion truncated is: ', prop, '\n')
    cat('Sample size: ', n, '\n')
    cat('Long covid patients: ', sum(df[['Long_COVID_diagnosis_post_covid_indicator']]), '\n')

    tmle_r <- get_tmle_est(fit)
    # change the column name to A and Y
    df <- df %>% rename(A = SSRI_Indicator, Y = Long_COVID_diagnosis_post_covid_indicator)

    v_subs <- var_robust_logRR(df, fit)$est
    v_iter_new <- var_robust_logRR(df, fit, update='iterative')$est
    v_one_new <- var_robust_logRR(df, fit, update='one-step')$est})
    print(c('logRR estimates', 'empirical', 'SS', 'iterative', 'one-step'))
    print(c(tmle_r[1], tmle_r[2], v_subs, v_iter_new, v_one_new))
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.468f2c2e-2273-4f2e-bbdc-973e23acedac"),
    sample2=Input(rid="ri.vector.main.execute.dc5f3fc4-e69b-4d1c-88f1-0e2101263bdb")
)
unnamed_2 <- function(sample2) {
    df <- sample2
    # include all the covariates
    cov_names <- colnames(df)
    cov_names <- cov_names[!cov_names %in% c('SSRI_Indicator', 'fracture_indicator', 'post_COVID_visit_indicator')]
    cov_terms <- paste0(cov_names, collapse='+')
    gform <- paste0('A~', cov_terms)
    Qform <- paste('Y~', 'A+', cov_terms)
    print(gform)
    print(Qform)
    SL.library = c("SL.glm", "tmle.SL.dbarts2", "SL.glmnet", "SL.xgboost", "SL.caret", "SL.caret.rpart", "SL.knn", "SL.nnet", "SL.randomForest", "SL.rpart") 
    fit <- tmle(Y = df[['fracture_indicator']],
              A = df[['SSRI_Indicator']],
              W = df %>% dplyr::select(-c(fracture_indicator, SSRI_Indicator)),
              gform = gform,
              Qform = Qform,
              Q.SL.library = SL.library,
              g.SL.library = SL.library,
              family = 'binomial')
              
    print(summary(fit))
    g1W <- fit$g$g1W
    hist(g1W)
    n <- length(g1W)
    alpha <- 5/sqrt(n)/log(n)
    prop <- sum((g1W < alpha) | (g1W > (1-alpha))) / n
    print(prop)

    tmle_r <- get_tmle_est(fit)
    # change the column name to A and Y
    df <- df %>% rename(A = SSRI_Indicator, Y = fracture_indicator)

    v_subs <- var_robust_logRR(df, fit)$est
    v_iter_new <- var_robust_logRR(df, fit, update='iterative')$est
    v_one_new <- var_robust_logRR(df, fit, update='one-step')$est
        
    print(c(tmle_r[1], tmle_r[2], v_subs, v_iter_new, v_one_new))   
}

