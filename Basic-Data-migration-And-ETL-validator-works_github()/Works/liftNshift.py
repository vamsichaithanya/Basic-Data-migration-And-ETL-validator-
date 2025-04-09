import os 
import pyodbc
import pandas as pd
import json
from sqlalchemy import create_engine

class liftNshift:
    
    def load_files(folderpath=None, file_Extension=None, file_path=None):
        dataframes = {}

        def read_file(file_path):
            file = os.path.basename(file_path)
            ext = file.split('.')[-1].lower()
            df_name = file.split('.')[0] + "_" + ext + "df"
            
            if ext == "json":
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif isinstance(data, dict):
                        df = pd.DataFrame.from_dict(data, orient="index").T
                    dataframes[df_name] = df  # Add missing assignment for JSON files
                except Exception as e:
                    print(f"Error reading JSON file {file_path}: {str(e)}")  # Added error message
            elif ext in run_case:
                try:
                    dataframes[df_name] = run_case[ext](file_path)
                except Exception as e:
                    print(f"Error reading file {file_path}: {str(e)}")  # Added error handling

        run_case = {
            "csv": pd.read_csv,
            "json": pd.read_json,  # Note: This is redundant since JSON has special handling above
            "xls": pd.read_excel,
            "xlsx": pd.read_excel
        }
                        
        if folderpath is None and file_Extension is None and file_path is None:
            dataframes["please enter enter FolerPath --and-- File_extention, --or-- File_path"] = None
            return dataframes
        
        elif folderpath is not None and file_Extension is None and file_path is None:
            for root, _, files in os.walk(folderpath):
                for file in files:
                    ext = file.split('.')[-1].lower()  # Fixed to get extension properly
                    if ext in run_case:
                        file_path = os.path.join(root, file)
                        read_file(file_path)
            return dataframes 

        elif folderpath is not None and file_Extension is not None and file_path is None:  
            ext = file_Extension.split('.')[-1].lower() if '.' in file_Extension else file_Extension  # Handle both ".csv" and "csv"
            for root, _, files in os.walk(folderpath):
                for file in files:
                    if file.endswith(file_Extension) or file.endswith('.' + file_Extension):   
                        file_path = os.path.join(root, file)
                        read_file(file_path)
            return dataframes

        elif folderpath is None and file_Extension is None and file_path is not None:
            ext = file_path.split('.')[-1].lower()
            if ext in run_case:
                read_file(file_path)
                return dataframes
            else:
                dataframes["Unsupported file type: " + ext] = None
                return dataframes
        else:
            dataframes["error occurs, Try Again"] = '0'
            return dataframes


    def printfiles(result):
        print(result.keys())
        print("*" * 50)
        for df_name, df in result.items():
            print(f"DataFrame_Name : {df_name}")
            print(f"DataFrame_Values : \n{df}\n\n")
            print("-" * 50)
        print("$" * 50)


    def exportfiles(result):
        server = 'IAMGROOT\SQLEXPRESS01'
        
        # Connect to SQL Server
        try:
            conn = pyodbc.connect(
                r"DRIVER={ODBC Driver 17 for SQL Server};"
                r"SERVER=IAMGROOT\SQLEXPRESS01;"
                r"Trusted_Connection=yes;", 
                autocommit=True
            )   
            cursor = conn.cursor()
        except pyodbc.Error as e:
            print(f"Connection Error: {str(e)}")
            return
        
        try:
            ans = input("want to dump dataframes to new database or existing database (new / existing) : \n").lower()
            
            if ans == 'new':
                newdatabase = input("enter newdatabase name : \n")
                query1 = f"IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'{newdatabase}') CREATE DATABASE [{newdatabase}]"
                query2 = f"USE [{newdatabase}]"
                cursor.execute(query1)
                cursor.execute(query2)
                database = newdatabase

            elif ans == 'existing':
                existingdatabase = input("enter existing database name : \n")
                query1 = f"USE [{existingdatabase}]"
                try:
                    cursor.execute(query1)
                    database = existingdatabase
                except pyodbc.Error as e:
                    print(f"Database error: {str(e)}")
                    cursor.close()
                    conn.close()
                    return
            else:
                print("Select options only from :\n1) new\n2) existing")
                cursor.close()
                conn.close()
                return
                
            # Create SQLAlchemy engine
            engine = create_engine(f"mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")
            
            # Export each DataFrame to SQL
            for tablename, df in result.items():
                if df is not None:  # Skip None values
                    try:
                        # Clean tablename to ensure SQL compatibility
                        tablename = ''.join(c if c.isalnum() or c == '_' else '_' for c in tablename)
                        df.to_sql(tablename, engine, if_exists='replace', index=False)
                        print(f"Successfully exported {tablename}")
                    except Exception as e:
                        print(f"Error exporting {tablename}: {str(e)}")
        
        except Exception as e:
            print(f"Error: {str(e)}")
        finally:
            cursor.close()
            conn.close()

