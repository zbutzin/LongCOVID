

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ad0c3e96-af8e-4b5d-8721-581870bed197"),
    data_partner_filtered=Input(rid="ri.foundry.main.dataset.f4d2580a-fd26-4e65-b432-bbe67e656668")
)
def Final_Table_1(data_partner_filtered):
    Joined_All_Patients = data_partner_filtered.toPandas()

    from datetime import datetime
    reference_date = datetime(2021, 10, 1)

# Function to calculate months difference
    def months_since_dec2021(date):
        return_int = (date.year - reference_date.year) * 12 + (date.month - reference_date.month)
        if return_int < 0:
            return None
        else:
            return return_int + 1

# Apply the function to calculate months
    Joined_All_Patients['months_from_october_2021'] = Joined_All_Patients['COVID_first_poslab_or_diagnosis_date'].apply(months_since_dec2021)
    # Preprocess_with_CCI = Preprocess_with_CCI.sort_values('Months_From_Jan_2021', ascending=False)
    # Preprocess_with_CCI = Preprocess_with_CCI.sort_values('Months_From_Jan_2021', ascending=False)
    return Joined_All_Patients

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f2ea8cd3-61e8-4681-a0ea-63082d962fae"),
    Final_Table_4=Input(rid="ri.foundry.main.dataset.359823b5-495e-4e3a-a71a-eb52679a151b")
)
def Final_Table_5(Final_Table_4):
    
    Joined_All_Patients = Final_Table_4.toPandas()

    from datetime import datetime
    reference_date = datetime(2018, 1, 1)

# Function to calculate months difference
    def months_since_dec2021(date):
        if not date:
            return None
        return_int = (date.year - reference_date.year) * 12 + (date.month - reference_date.month)
        if return_int < 0:
            return None
        else:
            return return_int + 1

# Apply the function to calculate months
    Joined_All_Patients['months_from_2018'] = Joined_All_Patients['drug_exposure_start_date'].apply(months_since_dec2021)
    # Preprocess_with_CCI = Preprocess_with_CCI.sort_values('Months_From_Jan_2021', ascending=False)
    # Preprocess_with_CCI = Preprocess_with_CCI.sort_values('Months_From_Jan_2021', ascending=False)
    return Joined_All_Patients
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.73912bdf-ad1d-4c83-9496-41433fbd18fa"),
    data_partner_info=Input(rid="ri.foundry.main.dataset.8cbad836-5750-4e94-8b98-e0d3ad6e9acb")
)
import numpy as np
def long_covid_filter(data_partner_info):
    data_partner_info = data_partner_info.toPandas()
    df = data_partner_info[['data_partner_id', 'Total_patients', 'Long_COVID_count', 'Long_COVID_proportion']]
    median = df['Long_COVID_proportion'].median()
    mean = df['Long_COVID_proportion'].mean()
    st_dev = df['Long_COVID_proportion'].std()
    df['above_cutoff'] = np.where(df['Long_COVID_proportion'] > (median - st_dev), 1, 0)
    print("Median:", median)
    print("Mean:", mean)
    print("Standard Deviation:", st_dev)
    print("Cutoff:", median - st_dev)
    print("Meets Criteria:", sum(df['above_cutoff']))
    print("Does not meet criteria:", np.count_nonzero(df['above_cutoff']==0))
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.db50c60b-34f4-438b-821d-4bebdc36e02f"),
    data_partner_info=Input(rid="ri.foundry.main.dataset.8cbad836-5750-4e94-8b98-e0d3ad6e9acb")
)
import numpy as np

def ssri_filter(data_partner_info):
    data_partner_info = data_partner_info.toPandas()
    df = data_partner_info[['data_partner_id', 'Total_patients', 'SSRI_count', 'SSRI_proportion']]
    median = df['SSRI_proportion'].median()
    mean = df['SSRI_proportion'].mean()
    st_dev = df['SSRI_proportion'].std()
    df['above_cutoff'] = np.where(df['SSRI_proportion'] > (median - st_dev), 1, 0)
    print("Median:", median)
    print("Mean:", mean)
    print("Standard Deviation:", st_dev)
    print("Cutoff:", median - st_dev)
    print("Meets Criteria:", sum(df['above_cutoff']))
    print("Does not meet criteria:", np.count_nonzero(df['above_cutoff']==0))
    return df
    

