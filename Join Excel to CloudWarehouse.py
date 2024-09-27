# %%
## importing packages
from azure.identity import InteractiveBrowserCredential
from datetime import datetime
import pyodbc
import struct
from itertools import chain, repeat
import pandas as pd
import matplotlib.pyplot as plt
import xlsxwriter   


# %%
current_time = datetime.now().strftime("%d%m%Y_%H%M%S") ## set time and date now
var0 = "XX" ## additional details for name of folder
var1 = "XX" ## Warehouse url link
var2 = r"XXx" ## excel postcode lookup - this must be one single list of postcodes for matching
export_folder_path = r"XXX" ##folder path
export_file_name = f"Data_{var0}_{current_time}.xlsx" ## name of the file which uses the current date and time as well as var0 which you can call what you want
var3 = f"{export_folder_path}\\{export_file_name}"
var4 = """
   SELECT 
    [Team],
    [ID,
    [DateofBird/EDD],
    [Gender],
    [WelshLevelofCare],
    [EYProgramme],
    [ChildAddress-Postcode],
    [Status]
FROM [ABB_PHN_Warehouse].[dbo].[ABB - HV - Integrated Birth Book (Datamart V2)]
WHERE [Status] = 'Active';
""" ## SQL Code from Warehouse

var5 = 'XX' ## DOB Code in Warehouse
var6_left = pd.to_datetime('2021-04-01')  ##DOB Range date one - from warehouse
var6_right = pd.to_datetime('2024-03-31') ## DOB Range too - from warehouse
var7 = 'XX' ##name of postcode in warehouse
var8 = 'Postcode' ##name of postcode in Excel
var_password = 'XX' ##password protect export

# %%
## This steps link to cloud to import data as a dataframe 
# This links to azure to log in to cloud enviroments that use this appraoch
credential = InteractiveBrowserCredential()

sql_endpoint = var1
database = "XX"

# Correct the connection string make sure that ODBC Driver 18 is installed or update this 
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_endpoint},1433;Database={database};Encrypt=Yes;TrustServerCertificate=No"


token_object = credential.get_token("https://database.windows.net//.default")
token_as_bytes = bytes(token_object.token, "UTF-8")


encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
attrs_before = {1256: token_bytes}

# Connect to the database
connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
cursor = connection.cursor()

# Execute the SQL script
cursor.execute(var4)

# Fetch the result into a Python table using pandas
rows = cursor.fetchall()
column_names = [column[0] for column in cursor.description]
sql_data = pd.DataFrame.from_records(rows, columns=column_names)

# Close cursor and connection
cursor.close()
connection.close()

# %%
# Read the Excel file
excel_path = var2
## Chang the above link to point to postcode file of choice
excel_data = pd.read_excel(excel_path)

# Remove all spaces from the entire DataFrame
excel_data = excel_data.applymap(lambda x: x.replace(' ', '') if isinstance(x, str) else x)

# %%
## cleaning data

excel_data = excel_data.applymap(lambda x: x.replace(' ', '') if isinstance(x, str) else x)
sql_data[var7] = sql_data[var7].apply(lambda x: x.replace(' ', '') if isinstance(x, str) else x)

sql_data[var5] = pd.to_datetime(sql_data[var5], errors='coerce')

# %%
##filter data
sql_data = sql_data[pd.to_datetime(sql_data[var5], errors='coerce').between(var6_left, var6_right)]



# %%
merged_data = pd.merge(sql_data, excel_data, left_on=var7, right_on=var8, how='inner') ## inner join described Only the rows with matching values in both DataFrames are included in the result




