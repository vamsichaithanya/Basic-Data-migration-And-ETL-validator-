import pandas as pd
from sql_def import sql
from liftNshift import liftNshift
from pathlib import Path

"""commented lines are example snippets that show project functionality"""

if __name__ == "__main__":

    input = Path(__file__).resolve().parent
         
    '''liftNshit'''

    # result=liftNshift.load_files(folderpath= rf"{input}\Datasets\liftNshift dataset")

    # result=liftNshift.load_files(folderpath= rf"{input}\Datasets\liftNshift dataset",file_Extension="csv")

    # result=liftNshift.load_files(file_path= rf"{input}\Datasets\liftNshift dataset\dwsample1-json.json")


    # liftNshift.printfiles(result)
    # liftNshift.exportfiles(result)



    '''validator'''

    # c =pd.read_csv(rf"{input}\Datasets\Validator dataset\file1.csv",low_memory=False)
    # a = c.iloc[:10000,:]
    # d=pd.read_csv(rf"{input}\Datasets\Validator dataset\file2.csv",low_memory=False)
    # b= d.iloc[:10000,:]

    # sql.validate_df(c,d,"Customer ID")
    # sql.validate_text(c,d,"Customer ID")
    # sql.validate_json(a,b,"Customer ID")

    # c =pd.read_csv(rf"{input}\Datasets\Validator dataset\file1_no_key.csv",low_memory=False)
    # a = c.iloc[:40000,:]
    # d=pd.read_csv(rf"{input}\Datasets\Validator dataset\file2_no_key.csv",low_memory=False)
    # b= d.iloc[:40000,:]

    # sql.validate_df(a,b)
    # sql.validate_text(a,b)
    # sql.validate_json(a,b)
    
    '''Sql_functions'''

    # file1=sql.select('file1_csvdf','ETL')
    # file2=sql.select('file2_csvdf','ETL')

    # sql.validate_df(file1,file2)



    # file1=sql.select('file1_for_assume_csvdf','ETL')
    # file2=sql.select('file2_for_assume_csvdf','ETL')

    # sql.validate_df(file1,file2)



    # file1=sql.select('file1_no_key_csvdf','ETL')
    # file2=sql.select('file2_no_key_csvdf','ETL')

    # sql.validate_df(file1,file2)



    # df_original = pd.DataFrame({
    # "ID": [101, 102, 103, 104, 105],
    # "Name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    # "Age": [25, 30, 35, 40, 22],
    # "City": ["New York", "Los Angeles", "Chicago", "Houston", "Miami"]  
    # })

    # sql.dump('sampletable1',df_original,'ETL')

    # df_original = sql.select('sampletable1','ETL')

    # df_new = df_original

    # df_new.loc[df_new["Name"] == "Bob", "Age"] = 32
    # df_new.loc[df_new["Name"] == "David", "City"] = "San Diego"
    # new_row = {"ID": 106, "Name": "Frank", "Age": 28, "City": "Seattle"}
    # new_row = {"ID": 106, "Name": "Frank", "Age": 28, "City": "Seattle"}
    # df_new = pd.concat([df_new, pd.DataFrame([new_row])], ignore_index=True)
    # df_new = df_new[df_new["ID"] != 105]
    # df_new.rename(columns={"City": "Location"}, inplace=True)

    # sql.checkdump('sampletable1',df_new,'ETL',df_original,'ID')











    df =pd.read_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file1.csv",low_memory=False)
    df = df.iloc[:20000,:]
    df.to_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file1.csv", index=False)

    df =pd.read_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file2.csv",low_memory=False)
    df = df.iloc[:20000,:]
    df.to_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file2.csv", index=False)

    df =pd.read_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file1_no_key.csv",low_memory=False)
    df = df.iloc[:20000,:]
    df.to_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file1_no_key.csv", index=False)

    df =pd.read_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file2_no_key.csv",low_memory=False)
    df = df.iloc[:20000,:]
    df.to_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file2_no_key.csv", index=False)

    df =pd.read_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file1_for_assume.csv",low_memory=False)
    df = df.iloc[:20000,:]
    df.to_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file1_for_assume.csv", index=False)

    df =pd.read_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file2_for_assume.csv",low_memory=False)
    df = df.iloc[:20000,:]
    df.to_csv(r"C:\Users\vamsi\Downloads\Validator dataset\file2_for_assume.csv", index=False)



    

