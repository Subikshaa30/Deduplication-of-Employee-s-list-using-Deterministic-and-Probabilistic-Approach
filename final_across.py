import requests
import pandas as pd
import recordlinkage
import warnings
import os
warnings.filterwarnings('ignore')

def block_and_compare(df, block_column, exact_columns=[], string_columns=[]):
    indexer = recordlinkage.Index()
    indexer.block(block_column)
    candidate_links = indexer.index(df)
    compare = recordlinkage.Compare()
    for column in exact_columns:
        compare.exact(column, column, label=column)
    for column in string_columns:
        compare.string(column, column, method='jarowinkler', threshold=0.95, label=column)    
    features = compare.compute(candidate_links, df)
    return features

def process_dataframes(df, block_columns, exact_columns, string_columns, score_threshold):
    processed_dfs = {}
    filtered_indexes = {}
    for block_column in block_columns:
        processed_df = block_and_compare(df, block_column, exact_columns, string_columns)
        columns_to_sum = exact_columns + string_columns
        processed_df['Consolidated_Score'] = processed_df[columns_to_sum].sum(axis=1)
        filtered_df = processed_df[processed_df['Consolidated_Score'] >= score_threshold]
        processed_dfs[block_column] = filtered_df
        filtered_indexes[block_column] = filtered_df.index
    return processed_dfs, filtered_indexes

def data_import(method, name):
    if method == 'self':
        df = pd.read_excel(fr'{name}.xlsx')
        return df
    elif method == 'across':
        import glob
        a = os.getcwd()
        os.chdir(a)       
        lists_of_docx = glob.glob('*.xlsx')
        print(lists_of_docx)
        dfs_names = []
        for i in range(1,len(lists_of_docx)+1):
            dfs_names.append('df_%i'%i)
        files = lists_of_docx[:] 
        dfs_names = (dfs_names)
        dfs = {}
        for dfn,file in zip(dfs_names, files):
            dfs[dfn] = pd.read_excel(file)
            print(dfs[dfn].shape)
        return dfs

def column_cleaning(data_frame):
    columns_to_remove = data_frame.columns[data_frame.isnull().all()].tolist()
    df_cleaned = data_frame.drop(columns=columns_to_remove, axis=1)
    return df_cleaned

def Donot_Removal(data_frame):
    filtered_df_Do_not = data_frame[data_frame[['LastName', 'MiddleName', 'FirstName']].apply(lambda x: x.str.contains('Do Not', case=False, na=False)).any(axis=1)]
    DoNot_index = [i for i in filtered_df_Do_not.index]
    Abstracted_DF = data_frame.drop(DoNot_index, axis = 0) 
    return Abstracted_DF, filtered_df_Do_not

def Test_Patients(data_frame):
    filtered_df_test = data_frame[data_frame[['LastName', 'MiddleName', 'FirstName']].apply(lambda x: x.str.contains('test', case=False, na=False)).any(axis=1)]
    test_index = [i for i in filtered_df_test.index]
    Abstracted_DF = data_frame.drop(test_index, axis = 0)
    return Abstracted_DF, filtered_df_test

def SSNs(data_frame):
    duplicates_ssn = data_frame[data_frame.duplicated('SSN', keep=False)] 
    duplicates_ssn['Duplicated'] = duplicates_ssn.duplicated('SSN', keep=False)
    Final_SSN_Duplicates = duplicates_ssn[duplicates_ssn['Duplicated']]
    Final_SSN_Duplicates = Final_SSN_Duplicates.sort_values(by='SSN')
    Final_SSN_Duplicates['Group_ID'] = (Final_SSN_Duplicates['SSN'] != Final_SSN_Duplicates['SSN'].shift()).cumsum()
    final_df = Final_SSN_Duplicates.reset_index().rename(columns={'index': 'Original_Index'})[['Original_Index', 'SSN', 'Group_ID']]
    final_df_cleaned = final_df.dropna(subset=['SSN'])
    final_df_cleaned.sort_values(by = 'SSN')
    index = [i for i in final_df_cleaned['Original_Index']]
    SSN = data_frame.loc[index]
    Abstracted_DF = data_frame.drop(index, axis = 0)    
    return Abstracted_DF, SSN
    
    
def names_DOB(data_frame, original_df, ABS, SSN_df):
    cols_to_check = ['FirstName', 'LastName', 'DOB', 'Gender']
    data_frame[cols_to_check] = data_frame[cols_to_check].apply(lambda x: x.astype(str).str.lower())
    duplicates_mask1 = data_frame.duplicated(subset=cols_to_check, keep=False)
    duplicates_df1 = original_df[duplicates_mask1]
    a = set(duplicates_df1['SSN'].unique())
    b = set(SSN_df['SSN'].unique())
    c = a.intersection(b)
    d = list(c)
    cleaned_names_df_wssn = duplicates_df1[~duplicates_df1['SSN'].isin(d)]
    names_index = [i for i in cleaned_names_df_wssn.index]
    ABS = ABS.drop(names_index, axis = 0)
    return ABS, cleaned_names_df_wssn

def cleaning_mod_2(data_frame):
    abss = data_frame[['PatientID', 'FirstName', 'LastName','DOB', 'Gender']]
    abss['DOB'] = pd.to_datetime(abss['DOB'])
    abss['DOB'] = abss['DOB'].dt.strftime('%m/%d/%Y')

    abss['FirstName'] = abss['FirstName'].astype(str)
    abss['LastName'] = abss['LastName'].astype(str)
    abss['DOB'] = abss['DOB'].astype(str)
    abss['Gender'] = abss['Gender'].astype(str)
    abss['PatientID'] = abss['PatientID'].astype(str)
    
    return abss

exact_columns = ['DOB', 'Gender']
string_columns = ['FirstName', 'LastName']
block_columns = ['FirstName', 'DOB', 'PatientID']
score_threshold = 3

df = data_import('across', 'sheet')

main_dfs = {}
for kys, dfrs in df.items():
    
    df_cleaned = column_cleaning(dfrs)
    Abstracted_DF, filtered_df_Do_not = Donot_Removal(df_cleaned)
    Abstracted_DF1, filtered_df_test = Test_Patients(Abstracted_DF)
    Abstracted_DF2, SSN1 = SSNs(Abstracted_DF1)
    Abstracted_DF3, cleaned_names_df_wssn = names_DOB(df_cleaned, dfrs, Abstracted_DF2, SSN1)
    
    abss = Abstracted_DF3[['PatientID', 'FirstName', 'LastName','DOB', 'Gender', 'SSN']]
    abss['DOB'] = pd.to_datetime(abss['DOB'])
    abss['DOB'] = abss['DOB'].dt.strftime('%m/%d/%Y')

    abss['FirstName'] = abss['FirstName'].astype(str)
    abss['LastName'] = abss['LastName'].astype(str)
    abss['DOB'] = abss['DOB'].astype(str)
    abss['Gender'] = abss['Gender'].astype(str)
    abss['PatientID'] = abss['PatientID'].astype(str)
    abss['SSN'] = abss['SSN'].astype(str)
    
    processed_dfs, filtered_indexes = process_dataframes(abss, block_columns, exact_columns, string_columns, score_threshold)

    results_dict = {}  
    rows_to_remove = []

    for key, df in processed_dfs.items():
        find_df = []
        filtered_df = df      
        for indexes in filtered_df.index:
            extracted_rows = abss.loc[abss.index.isin(indexes)]   
            extracted_rows['Gender'] = extracted_rows['Gender'].astype(str).str.lower()
            if len(extracted_rows)> 1 and extracted_rows.iloc[0]['DOB'] == extracted_rows.iloc[1]['DOB']:
                find_df.append(extracted_rows)    
                rows_to_remove.extend(extracted_rows.index.tolist())
        if find_df:
            results_dict[key] = pd.concat(find_df)
        else:
            pass
        
    b = set(rows_to_remove)
    abss = abss.drop(list(b), axis = 0)    
    print(results_dict.keys())    
    main_dfs[kys] = {
        'filtered_df_Do_not': filtered_df_Do_not,
        'filtered_df_test': filtered_df_test,
        'SSN1': SSN1,
        'cleaned_names_df_wssn': cleaned_names_df_wssn,
        'Abstracted_DF3': abss,
        'results':results_dict
    }
    
columns_name = ['FirstName', 'LastName', 'SSN', 'Gender', 'DOB']

do_not_dfs = {}
for key in main_dfs:
    do_not_dfs[key] = main_dfs[key]['filtered_df_Do_not']
    
final_df_donot = pd.DataFrame()
for key, dataframe in do_not_dfs.items():
    dataframe = dataframe[columns_name] if all(col in dataframe for col in columns_name) else pd.DataFrame(columns=columns_name)
    dataframe.columns = [f"{col}_{key}" for col in columns_name]    
    if final_df_donot.empty:
        final_df_donot = dataframe
    else:
        final_df_donot = pd.concat([final_df_donot, dataframe], axis=1)
        

test_dfs = {}
for key in main_dfs:
    test_dfs[key] = main_dfs[key]['filtered_df_test']
    
final_df_test = pd.DataFrame()
for key, dataframe in test_dfs.items():
    dataframe = dataframe[columns_name] if all(col in dataframe for col in columns_name) else pd.DataFrame(columns=columns_name)
    dataframe.columns = [f"{col}_{key}" for col in columns_name]    
    if final_df_test.empty:
        final_df_test = dataframe
    else:
        final_df_test = pd.concat([final_df_test, dataframe], axis=1)
        
SSN1_dfs = {}
for key in main_dfs:
    SSN1_dfs[key] = main_dfs[key]['SSN1']
    
final_df_ssn = pd.DataFrame()
for key, dataframe in SSN1_dfs.items():
    dataframe = dataframe[columns_name] if all(col in dataframe for col in columns_name) else pd.DataFrame(columns=columns_name)
    dataframe.columns = [f"{col}_{key}" for col in columns_name]    
    if final_df_ssn.empty:
        final_df_ssn = dataframe
    else:
        final_df_ssn = pd.concat([final_df_ssn, dataframe], axis=1)
        
cleaned_names_df_wssn_dfs = {}
for key in main_dfs:
    cleaned_names_df_wssn_dfs[key] = main_dfs[key]['cleaned_names_df_wssn']
    
final_df_cleaned_names_df_wssn = pd.DataFrame()
for key, dataframe in cleaned_names_df_wssn_dfs.items():
    dataframe = dataframe[columns_name] if all(col in dataframe for col in columns_name) else pd.DataFrame(columns=columns_name)
    dataframe.columns = [f"{col}_{key}" for col in columns_name]    
    if final_df_cleaned_names_df_wssn.empty:
        final_df_cleaned_names_df_wssn = dataframe
    else:
        final_df_cleaned_names_df_wssn = pd.concat([final_df_cleaned_names_df_wssn, dataframe], axis=1)
        
Abstracted_DF3_dfs = {}
for key in main_dfs:
    Abstracted_DF3_dfs[key] = main_dfs[key]['Abstracted_DF3']
    
valid = pd.DataFrame()
for key, dataframe in Abstracted_DF3_dfs.items():
    dataframe = dataframe[columns_name] if all(col in dataframe for col in columns_name) else pd.DataFrame(columns=columns_name)
    dataframe.columns = [f"{col}_{key}" for col in columns_name]    
    if valid.empty:
        valid = dataframe
    else:
        valid = pd.concat([valid, dataframe], axis=1)
        
result_dfs = {}
for key, dataframe in main_dfs.items():
    for key1, dataframe1 in main_dfs[key]['results'].items():
        name = f"{key}_{key1}"
        result_dfs[name] = dataframe1
        print(name)
        
result = {}
for key, df2 in result_dfs.items():
    parts = key.split('_')
    df_index = parts[1]  
    column_name = parts[2]
    renamed_columns = {name: f"{name}_df_{df_index}" for name in df.columns}
    df2 = df2.rename(columns=renamed_columns)
    if column_name not in result:
        result[column_name] = []
    result[column_name].append(df2)
final_dfs = {name: pd.concat(dfs, axis=1) for name, dfs in result.items()}

excel_path = 'Across_EMRs.xlsx'
with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
    final_df_donot.to_excel(writer, sheet_name='Do Not Use')
    final_df_test.to_excel(writer, sheet_name='Test Patients')
    final_df_ssn.to_excel(writer, sheet_name='Duplicates w.r.to SSN')
    final_df_cleaned_names_df_wssn.to_excel(writer, sheet_name='Duplicates w.r.to Names')
    valid.to_excel(writer, sheet_name='Valid Patients')
    for features in final_dfs.keys():
        final_dfs[features].to_excel(writer, sheet_name = f"{features}_Duplicates.(Check this carefully)")