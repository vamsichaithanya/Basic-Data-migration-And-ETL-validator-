import time
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from hashing_and_parallel_validator import validate as v1

class sql:

    primary_key=None
    assume = None
    flag = 0

    def conn(dbname):
            return pyodbc.connect(
            r"DRIVER={ODBC Driver 17 for SQL Server};"
            r"SERVER=IAMGROOT\SQLEXPRESS01;"
            fr"DATABASE={dbname};"
            r"Trusted_Connection=yes;",
            autocommit=True
        )   
             
    def engine(database):
        return create_engine(fr"mssql+pyodbc://IAMGROOT\SQLEXPRESS01/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")
  
    def select(table,dbname): 
        query=f"select * from [{table}]"
        df = pd.read_sql(query,sql.engine(dbname))

        if sql.primary_key == None :
            sql.getprimarykey(table,dbname)

        return df
    
    def showtables(dbname):
        conn= sql.conn(dbname)
        query="SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
        df = pd.read_sql(query,conn)
        conn.close()

        return df
    
    def getprimarykey(table, dbname):
        """Fetch primary key from SQL metadata or determine it based on uniqueness."""        
        if sql.flag == 0:
            print("Searching for primary key in DataBase")
            engine=sql.engine(dbname)
            query=f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{table}' AND CONSTRAINT_NAME IN (SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = '{table}' AND CONSTRAINT_TYPE = 'PRIMARY KEY');"
            df = pd.read_sql(query,engine)

            if not df.empty:
                sql.primary_key = df.iloc[0, 0]
                print(f"Found primary key as : {sql.primary_key}")
                return sql.primary_key
            
            else: 
                print(f"No Primary key Found in Data Base \nSearching Unique Column to use as primary key")
                engine = sql.engine(dbname)
                query = f"""
                        declare @table_name nvarchar(max) = '{table}';
                        declare @col_name nvarchar(max);
                        declare @sql nvarchar(max) = '';

                        declare col_cursor cursor for 
                        select column_name from information_schema.columns where table_name = @table_name;

                        open col_cursor;
                        fetch next from col_cursor into @col_name;

                        while @@fetch_status = 0
                        begin
                            set @sql = @sql + 
                                'select ''' + @col_name + ''' as columnname, count(*) - count(distinct [' + @col_name + ']) as duplicatecount, count(*) as total_rows from [' + @table_name + '] union all ';

                            fetch next from col_cursor into @col_name;
                        end

                        close col_cursor;
                        deallocate col_cursor;

                        set @sql = left(@sql, len(@sql) - 10);

                        set @sql = 'select *  from ('+@sql+') as temp where [duplicatecount] = (select min(duplicatecount) from (' + @sql + ') as subquery)'

                        exec sp_executesql @sql;
                        """

                df = pd.read_sql(query, engine)

                column_name = df.iloc[0]["columnname"]
                duplicate_count = df.iloc[0]["duplicatecount"]
                total_rows = df.iloc[0]["total_rows"]

                if ((1-(duplicate_count / total_rows )) == 1):

                    sql.primary_key = column_name
                    sql.assume=True   
                else:
                    sql.primary_key = None
                    sql.flag = 1
    
        return sql.primary_key

    def dump(table,dfname,dbname):

        engine=sql.engine(dbname)
        dfname.to_sql(table,engine,if_exists='replace', index=False)

        print("dumped")
    
    def validate_json(df_original, df_new, primary_key = None):
        start_time = time.time()

        if primary_key == None:
            primary_key = sql.primary_key

            if primary_key==None:
                diff = v1.check_diff(df_original, df_new)
            else:    
                diff = v1.check_diff(df_original, df_new, primary_key)
        else:    
            diff = v1.check_diff(df_original, df_new, primary_key)  

        import json
        from pathlib import Path
        print(f"Loading data into json file..", end="", flush=True)

        BASE_DIR = Path(__file__).resolve().parent
        out=BASE_DIR/"Validator_output"/"csv_differences.json"

        with open(out, 'w') as f:
            json.dump(diff, f, indent=2)
        print(f"\nDetailed results exported to {out}")

        elapsed_time = time.time() - start_time
        minutes = (elapsed_time % 3600) // 60
        seconds = elapsed_time % 60
        milliseconds = (seconds - int(seconds)) * 1000

        print(f"\nProcess finished in {int(minutes)}m {int(seconds)}s {int(milliseconds):.0f}ms")
 
    def validate_df(df_original, df_new, primary_key = None):

        start_time = time.time()

        if primary_key == None:
            primary_key = sql.primary_key

            if primary_key==None:
                diff = v1.check_diff(df_original, df_new)
            else:    
                diff = v1.check_diff(df_original, df_new, primary_key)
        else:
            diff = v1.check_diff(df_original, df_new, primary_key)

        diff = v1.get_dataframe_results(df_original, df_new, diff)
        if primary_key == None:
            sql.method = None

        elif(sql.assume==True):

            sql.method="assume"  
        else:
            sql.method = "key"
            

        v1.print_df_output(diff,sql.method)

        elapsed_time = time.time() - start_time
        minutes = (elapsed_time % 3600) // 60
        seconds = elapsed_time % 60
        milliseconds = (seconds - int(seconds)) * 1000

        print(f"\nProcess finished in {int(minutes)}m {int(seconds)}s {int(milliseconds):.0f}ms")

    def validate_text(df_original, df_new, primary_key=None):

        start_time = time.time()
        if primary_key == None:
            primary_key = sql.primary_key

            if primary_key==None:
                diff = v1.check_diff(df_original, df_new)
                a=v1.print_formatted_output(diff)
            else:
                diff = v1.check_diff(df_original, df_new, primary_key)
                a=v1.print_formatted_output(diff)
        else:
            diff = v1.check_diff(df_original, df_new, primary_key)
            a=v1.print_formatted_output(diff)

        if primary_key == None:
            sql.method = None

        elif(sql.assume==True):

            sql.method="assume"  
        else:
            sql.method = "key"
            
        if a =="yes": 
            diff = v1.get_dataframe_results(df_original, df_new,diff)
            v1.print_df_output(diff,sql.method)

        elapsed_time = time.time() - start_time
        minutes = (elapsed_time % 3600) // 60
        seconds = elapsed_time % 60
        milliseconds = (seconds - int(seconds)) * 1000

        print(f"\nProcess finished in {int(minutes)}m {int(seconds)}s {int(milliseconds):.0f}ms")
          
    def checkdump(table,df_new,dbname,df_original,primary_key=None):
        start_time = time.time()
        if primary_key == None:
            primary_key = sql.primary_key

            if primary_key==None:
                diff = v1.validate_csv_differences(df_original,df_new, primary_key)
            else:
                diff = v1.validate_csv_differences(df_original,df_new, primary_key)
        else:
            diff = v1.validate_csv_differences(df_original,df_new, primary_key)

        elapsed_time = time.time() - start_time
        minutes = (elapsed_time % 3600) // 60
        seconds = elapsed_time % 60
        milliseconds = (seconds - int(seconds)) * 1000

        print(f"\nProcess finished in {int(minutes)}m {int(seconds)}s {int(milliseconds):.0f}ms")

        if (v1.value(diff)>0):
            print("\nData had modified. \nDo you want to dump it?\n")
            print("To dump: Enter 1\nTo check data: Enter 2\nEnter any other key to cancel\n")
            a= input()
            if a=="1":
               sql.dump(table,df_new,dbname)
            elif a=="2":
                if primary_key == None:
                   sql.validate_df(df_original, df_new)
                else:
                   sql.validate_df(df_original,df_new,primary_key)

                print("\nDo you still want to dump data?")
                print("To dump: Enter 1\nEnter any other key to cancel\n")
                b =input()
                if b == "1":
                   sql.dump(table,df_new,dbname)
        else :
            print("Data has not been modified. \nDo you still want to dump it?\n")
            print("To dump: Enter 1\nEnter any other key to cancel\n")
            a= input()
            if a=="1":
                df=pd.DataFrame(df_new)
                sql.dump(table,df,dbname)