library(dplyr)
# library(SuperLearner)
# library(caret)
# library(glmnet)
# library(randomForest)
# library(ranger)
# library(xgboost)
library(ltmle)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4eac6916-c300-4c95-a6ac-626fd007f027"),
    Preprocess=Input(rid="ri.foundry.main.dataset.9ebd355d-f278-42fe-ab27-34582ac316f5")
)
AAPI <- function(Preprocess) {
    df <- Preprocess
    df$race_ethnicity_AAPI <- df$race_ethnicity_asian_non_hispanic + df$race_ethnicity_native_hawaiian_or_other_pacific_islander_non_hispanic
    df <- df %>% select(-c(race_ethnicity_asian_non_hispanic, race_ethnicity_native_hawaiian_or_other_pacific_islander_non_hispanic))

    return(df)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3efdee97-d5a0-4ae1-b92f-d8a99eea86e6"),
    sum_symptoms=Input(rid="ri.foundry.main.dataset.21edab8a-a57f-470f-91ab-8ab7972adfae")
)
ltmle_func <- function(sum_symptoms) {
    # W  —> A1 (race) —> C1 (monitoring) -> A2 (symptoms: 0 or not) —> Y
    # convert character column to factor   
    df = sum_symptoms
    df_results <- data.frame(matrix(ncol = 5, nrow = 0))
    censorC <- BinaryToCensoring(is.censored = df['C1'])
    df['C1'] <- censorC

    # regime
    print('Start building regimes')
    libraries <- c('glm')
    # A1 (gender) —> A2 (symptoms: 0 or not)
    abar <- list(c(1, 1), c(0, 1)) 
    print('Start modeling')
    A_columns <- grep("race_ethnicity", names(df), value = TRUE)
    
    print(abar)
    for(A_c in A_columns){
        if(A_c != 'race_ethnicity_white_non_hispanic'){
            print(A_c)
            dropped_race <- A_columns[A_columns != A_c]
            df_temp <- df %>% filter(.[[A_c]] == 1 | race_ethnicity_white_non_hispanic == 1) %>% 
                            select(-dropped_race) %>% 
                            rename(A2 = symptom_ind, Y=long_covid_indicator) 
            colnames(df_temp)[colnames(df_temp) == A_c] <- 'A1'
                            
            # print(colnames(df_temp))
            print(nrow(df_temp))
            r = ltmle(data = df_temp, Anodes=c('A1', 'A2'), Cnodes='C1', Ynodes='Y', 
                    survivalOutcome=TRUE, 
                    abar=abar,
                    SL.library=libraries)
            summary_r <- summary(r)$effect.measures
            # print(summary(r))
            print(summary_r)
            result <- c(A_c, summary_r$RR$estimate, summary_r$RR$CI[1], summary_r$RR$CI[2], summary_r$RR$pvalue)
            df_results <- rbind(df_results, result)
        }
    }
    colnames(df_results) <- c('Race', 'RR', 'RR_CI_lower', 'RR_CI_upper', 'p_value')
    return(df_results) 
}

