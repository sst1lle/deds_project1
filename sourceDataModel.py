import pandas as pd
import sqlite3
import warnings
import pyodbc
import numpy as np
import os

warnings.simplefilter('ignore')
print(pyodbc.drivers())


# Path to the folder containing the CSV files
csv_folder = "databases/AdventureWorks"

# List all CSV files in the folder
csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
print(f"Found CSV files: {csv_files}")

# Initialize a dictionary to store DataFrames
df_AdventureWorks = {}

# Loop through each CSV file and load it into a DataFrame
for csv_file in csv_files:
    file_path = os.path.join(csv_folder, csv_file)
    print(f"Loading file: {file_path}")
    try:
        # Try reading the file with a fallback encoding
        df = pd.read_csv(file_path, encoding='latin1')  # Use 'latin1' or 'iso-8859-1' if UTF-8 fails
        # Store the DataFrame in the dictionary with the table name as the key
        table_name = os.path.splitext(csv_file)[0]
        df_AdventureWorks[table_name] = df
    except Exception as e:
        print(f"Failed to load {csv_file}: {e}")

# Access individual DataFrames by their table name
for table_name, df in df_AdventureWorks.items():
    print(f"Table: {table_name}, Rows: {len(df)}\n{df.head()}\n")
    print("Columns:", df.columns.tolist())


#function to clean nan values
def clean_nan_values(dw):
    for table_name, df in dw.items():
        # Zet alle NaN naar None zodat SQL Server NULL kan verwerken
        dw[table_name] = df.astype(object).where(pd.notnull(df), None)
    return dw

# Clean NaN values
df_AdventureWorks = clean_nan_values(df_AdventureWorks)


# Path to the SQLite database
sqlite_file = "databases/aenc.sqlite"

# Connect to the SQLite database
conn = sqlite3.connect(sqlite_file)

# Get a list of all tables in the database
query = "SELECT name FROM sqlite_master WHERE type='table';"
tables = pd.read_sql(query, conn)
table_names = tables['name'].tolist()
print(f"Found tables: {table_names}")

# Initialize a dictionary to store DataFrames
df_aenc = {}

# Loop through each table and load it into a DataFrame
for table_name in table_names:
    print(f"Loading table: {table_name}")
    try:
        # Read the table into a DataFrame
        df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
        # Store the DataFrame in the dictionary with the table name as the key
        df_aenc[table_name] = df
    except Exception as e:
        print(f"Failed to load table {table_name}: {e}")

# Close the database connection
conn.close()

# Access individual DataFrames by their table name
for table_name, df in df_aenc.items():
    print(f"Table: {table_name}, Rows: {len(df)}\n{df.head()}\n")


# Clean NaN values in the DataFrames
df_aenc = clean_nan_values(df_aenc)


# Inlezen database: NorthWind SQL server
# Verbindingsgegevens
server = '127.0.0.1'        
port = '1433'               
database = 'NorthWind'         
username = 'SA'             
password = 'iDTyjZx7dRL4'  

# Connection string
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server},{port};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
    "Timeout=30;"
)

# Maak verbinding met de database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Haal alle tabellen op
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
tables = [row.TABLE_NAME for row in cursor.fetchall()]

# Dictionary om alle dataframes op te slaan
df_NorthWind = {}

# Loop door alle tabellen en laad ze in Pandas DataFrames
for table in tables:
    query = f"SELECT * FROM [{table}]"
    df = pd.read_sql(query, conn)
    df_NorthWind[table] = df
    print(f"Tabel '{table}' ingelezen met {df.shape[0]} rijen en {df.shape[1]} kolommen.")

# Sluit de verbinding
conn.close()

# Print de kolomnamen en de eerste paar rijen van elke DataFrame
for table_name, df in df_NorthWind.items():
    print(f"\nTable: {table_name}")
    print("Columns:", df.columns.tolist())
    print(df.head())
    
#maak dictionary aan voor alle goede dataframes + mapping dictionary
dfs_sourcedatamodel = {}
df_mapping = {}

#table: Production_ProductCategory
#bronnen: Production_ProductCategory + Categories


# Data opnieuw laden om dubbele kolommen te voorkomen
df_product_category = df_AdventureWorks.get("Production_ProductCategory").copy()
df_categories = df_NorthWind.get("Categories").copy()

# Voeg 'Source' kolom toe
df_product_category['Source'] = 'AdventureWorks'
df_categories['Source'] = 'NorthWind'


# Voeg een unieke sleutel toe (MergedCategoryID) als deze nog niet bestaat
if "MergedCategoryID" not in df_product_category.columns:
    df_product_category.insert(0, "MergedCategoryID", range(1, len(df_product_category) + 1))

if "MergedCategoryID" not in df_categories.columns:
    df_categories.insert(0, "MergedCategoryID", range(len(df_product_category) + 1, len(df_product_category) + len(df_categories) + 1))

# Hernoem kolommen zodat ze overeenkomen
df_categories.rename(columns={
    "CategoryID": "ProductCategoryID", 
    "CategoryName": "Name"
}, inplace=True)

# Combineer de twee tabellen onder elkaar
merged_df_productCategorie = pd.concat([df_product_category, df_categories], ignore_index=True)

# Vul NaN-waarden in met lege strings (optioneel)
merged_df_productCategorie.fillna("", inplace=True)

# Verwijder de kolom ShipDate als deze bestaat
if "Picture" in merged_df_productCategorie.columns:
    merged_df_productCategorie = merged_df_productCategorie.drop(columns=["Picture"])


#mapping van de categorieen per bron
category_mapping = merged_df_productCategorie[['ProductCategoryID', 'Source', 'MergedCategoryID']]

dfs_sourcedatamodel["Production_ProductCategory"] = merged_df_productCategorie
df_mapping["category_mapping"] = category_mapping




# Print het resultaat
print(merged_df_productCategorie)



#table: Production_Product
#bronnen: Production_Product + Products + Product

# Functie om alle datums correct te converteren
def convert_dates(df):
    date_columns = [col for col in df.columns if "date" in col.lower()]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')  # Zet om naar datetime
    return df

# Functie om -1 te vervangen door None
def clean_negative_values(df):
    for col in df.select_dtypes(include=[np.number]).columns:
        df[col] = df[col].apply(lambda x: None if x == -1 else x)
    return df

# Data opnieuw laden om dubbele kolommen te voorkomen
df_production_product = df_AdventureWorks.get("Production_Product").copy()
df_products = df_NorthWind.get("Products").copy()
df_product = df_aenc.get("Product").copy()

# Voeg 'Source' kolom toe
df_production_product['Source'] = 'AdventureWorks'
df_products['Source'] = 'NorthWind'
df_product['Source'] = 'AENC'

# Maak een unieke MergedID voor elke rij
df_production_product.insert(0, "MergedID", range(1, len(df_production_product) + 1))
df_products.insert(0, "MergedID", range(len(df_production_product) + 1, len(df_production_product) + len(df_products) + 1))
df_product.insert(0, "MergedID", range(len(df_production_product) + len(df_products) + 1, 
                                       len(df_production_product) + len(df_products) + len(df_product) + 1))

# Zorg dat alle tabellen dezelfde kolommen hebben
df_products.rename(columns={"ProductName": "Name", "CategoryID": "ProductCategoryID", "UnitPrice": "ListPrice"}, inplace=True)
df_product.rename(columns={"id": "ProductID", "name": "Name", "prod_size": "Size", "unit_price": "ListPrice", "Category": "ProductCategoryID"}, inplace=True)

# Voeg ontbrekende kolommen toe met lege waarden
all_columns = set(df_production_product.columns).union(set(df_products.columns)).union(set(df_product.columns))
for df in [df_production_product, df_products, df_product]:
    for col in all_columns:
        if col not in df.columns:
            df[col] = None

# Zet alle tabellen onder elkaar
merged_df_product = pd.concat([df_production_product, df_products, df_product], ignore_index=True)

# Datumconversie toepassen
merged_df_product = convert_dates(merged_df_product)

# Negatieve waarden verwijderen
merged_df_product = clean_negative_values(merged_df_product)

# Verwijder 'picture_name' en 'DiscontinuedDate' kolommen
merged_df_product.drop(columns=["picture_name"], inplace=True, errors="ignore")

# Hernoem 'ReorderLevel' naar 'ReorderPoint' en zet ze samen in één kolom
if "ReorderLevel" in merged_df_product.columns and "ReorderPoint" in merged_df_product.columns:
    merged_df_product["ReorderPoint"] = merged_df_product["ReorderPoint"].combine_first(merged_df_product["ReorderLevel"])
    merged_df_product.drop(columns=["ReorderLevel"], inplace=True)

# Hernoem 'color' zodat beide versies samengevoegd worden in één kolom
if "color" in merged_df_product.columns and "Color" in merged_df_product.columns:
    merged_df_product["Color"] = merged_df_product["Color"].combine_first(merged_df_product["color"])
    merged_df_product.drop(columns=["color"], inplace=True)

product_mapping = merged_df_product[['ProductID', 'Source', 'MergedID']]

# Merge op ProductCategoryID + Source om conflicten te vermijden
merged_df_product = merged_df_product.merge(
    category_mapping,
    how='left',
    left_on=['ProductCategoryID', 'Source'],
    right_on=['ProductCategoryID', 'Source']
)

# Vervang de oude ProductCategoryID door de nieuwe MergedCategoryID
merged_df_product['ProductCategoryID'] = merged_df_product['MergedCategoryID']
merged_df_product.drop(columns=['MergedCategoryID'], inplace=True, errors='ignore')

# Verwijder de kolom ShipDate als deze bestaat
if "DiscontinuedDate" in merged_df_product.columns:
    merged_df_product = merged_df_product.drop(columns=["DiscontinuedDate"])

# Data opslaan in het data warehouse dictionary
dfs_sourcedatamodel["Production_Product"] = merged_df_product
df_mapping["product_mapping"] = product_mapping

# Print het resultaat
print(merged_df_product)


#table: Purchasing_Vendor

#get dataframe from dictionary
purchasing_vendor_df = df_AdventureWorks.get("Purchasing_Vendor")

# Check if the DataFrame exists
if purchasing_vendor_df is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Purchasing_Vendor"] = purchasing_vendor_df

    print(f"Rows: {len(purchasing_vendor_df)}")
    print(purchasing_vendor_df.head())
else:
    print("The table 'Purchasing_Vendor' does not exist in database.")


#table: sales_store



#get dataframe from dictionary
sales_store_df = df_AdventureWorks.get("Sales_Store")

# Check if the DataFrame exists
if sales_store_df is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Sales_Store"] = sales_store_df

    print(f"Rows: {len(sales_store_df)}")
    print(sales_store_df.head())
else:
    print("The table does not exist in database.")



#tabel: employee_territories

#get dataframe from dictionary
employeeTerritories_df = df_NorthWind.get("EmployeeTerritories")

# Check if the DataFrame exists
if employeeTerritories_df is not None:
     # Apply the same offset as Employee_df2 for AENC employees
    employeeTerritories_df["EmployeeID"] = employeeTerritories_df["EmployeeID"].astype(int) + 100000  
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["EmployeeTerritories"] = employeeTerritories_df

    print(f" Rows: {len(employeeTerritories_df)}")
    print(employeeTerritories_df.head())
else:
    print("The table does not exist in database.")


#table: Territories

#get dataframe from dictionary
territories_df = df_NorthWind.get("Territories")

# Check if the DataFrame exists
if territories_df is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Territories"] = territories_df

    print(f" Rows: {len(territories_df)}")
    print(territories_df.head())
else:
    print("The table does not exist in database.")


#table: Region
#get dataframe from dictionary
region_df = df_NorthWind.get("Region")

# Check if the DataFrame exists
if region_df is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Region"] = region_df

    print(f" Rows: {len(region_df)}")
    print(region_df.head())
else:
    print("The table does not exist in database.")


#Table: Bonus

# Get dataframe from dictionary
bonus_df = df_aenc.get("Bonus")
bonus_df = bonus_df.drop_duplicates(subset=["emp_id", "bonus_date"])


# Check if the DataFrame exists
if bonus_df is not None:
    # Apply the same offset as Employee_df2 for AENC employees
    bonus_df["emp_id"] = bonus_df["emp_id"].astype(int) + 200000  

    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Bonus"] = bonus_df

    print(f"Rows: {len(bonus_df)}")
    print(bonus_df.head())
else:
    print("The table does not exist in database.")


#table: Sales_Customer
#Bronnen: Customer + Customers + Sales_Customer


#get dataframe from dictionary
Sales_Customer_df = df_AdventureWorks.get("Sales_Customer")


# Laden van de datasets (vervang dit met je eigen DataFrame-imports)
Sales_Customer_df = df_AdventureWorks.get("Sales_Customer")
customer_df = df_aenc.get("Customer")   
customers_df = df_NorthWind.get("Customers")
# Voeg 'Source' kolom toe
Sales_Customer_df["Source"] = "AdventureWorks"
customer_df["Source"] = "AENC"
customers_df["Source"] = "NorthWind"

# Zorg dat CustomerID overal een string is om inconsistenties te voorkomen
Sales_Customer_df["CustomerID"] = Sales_Customer_df["CustomerID"].astype(str)
customers_df["CustomerID"] = customers_df["CustomerID"].astype(str)
customer_df.rename(columns={"id": "CustomerID"}, inplace=True)
customer_df["CustomerID"] = customer_df["CustomerID"].astype(str)

# Hernoem kolommen om aan te sluiten bij Sales_Customer structuur
customers_df.rename(columns={"CompanyName": "CompanyName"}, inplace=True)
customer_df.rename(columns={"company_name": "CompanyName"}, inplace=True)

# Houd alleen de relevante kolommen
Sales_Customer_df = Sales_Customer_df[["CustomerID", "PersonID", "StoreID", "TerritoryID", "AccountNumber", "rowguid", "ModifiedDate", "Source"]]
customer_df = customer_df[["CustomerID", "CompanyName", "Source"]]
customers_df = customers_df[["CustomerID", "CompanyName", "Source"]]

# Voeg een unieke sleutel toe (MergedCustomerID)
Sales_Customer_df.insert(0, "MergedCustomerID", range(1, len(Sales_Customer_df) + 1))
customer_df.insert(0, "MergedCustomerID", range(len(Sales_Customer_df) + 1, len(Sales_Customer_df) + len(customer_df) + 1))
customers_df.insert(0, "MergedCustomerID", range(len(Sales_Customer_df) + len(customer_df) + 1, len(Sales_Customer_df) + len(customer_df) + len(customers_df) + 1))

# Combineer de tabellen onder elkaar
merged_customers = pd.concat([Sales_Customer_df, customer_df, customers_df], ignore_index=True)

# Vul NaN-waarden in met None (SQL-compatible)
merged_customers = merged_customers.where(pd.notna(merged_customers), None)

# Mapping maken voor toekomstige foreign key updates
customer_mapping = merged_customers[["CustomerID", "Source", "MergedCustomerID"]]



dfs_sourcedatamodel["Sales_Customer"] = merged_customers
df_mapping["customer_mapping"] = customer_mapping



# Print het resultaat
print(merged_customers)


#table: Sales_SalesTerritory


#get dataframe from dictionary
Sales_SalesTerritory_df = df_AdventureWorks.get("Sales_SalesTerritory")
Sales_SalesTerritory_df.rename(columns={"Group": "Group1"}, inplace=True)

# Check if the DataFrame exists
if Sales_SalesTerritory_df is not None:

    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Sales_SalesTerritory"] = Sales_SalesTerritory_df

    print(f" Rows: {len(Sales_SalesTerritory_df)}")
    print(Sales_SalesTerritory_df.head())
else:
    print("The table does not exist in database.")




#table: Purchasing_PurchaseOrderHeader


#get dataframe from dictionary
Purchasing_PurchaseOrderHeader = df_AdventureWorks.get("Purchasing_PurchaseOrderHeader")

# Check if the DataFrame exists
if Purchasing_PurchaseOrderHeader is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Purchasing_PurchaseOrderHeader"] = Purchasing_PurchaseOrderHeader

    print(f" Rows: {len(Purchasing_PurchaseOrderHeader)}")
    print(Purchasing_PurchaseOrderHeader.head())
else:
    print("The table does not exist in database.")


#table: Purchasing_PurchaseOrderDetail

# Get the Purchasing_PurchaseOrderDetail DataFrame from the dictionary
Purchasing_PurchaseOrderDetail = df_AdventureWorks.get("Purchasing_PurchaseOrderDetail")

# Check if the DataFrame exists
if Purchasing_PurchaseOrderDetail is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Purchasing_PurchaseOrderDetail"] = Purchasing_PurchaseOrderDetail

    # Perform the merge to replace ProductID with MergedID
    Purchasing_PurchaseOrderDetail = Purchasing_PurchaseOrderDetail.merge(
        product_mapping,  # This contains ProductID, Source, and MergedID
        how='left',
        left_on=['ProductID'],  # Assuming ProductID exists in this table
        right_on=['ProductID']
    )

    # Replace ProductID with MergedID
    Purchasing_PurchaseOrderDetail['ProductID'] = Purchasing_PurchaseOrderDetail['MergedID']

    # Drop the MergedID column as it's no longer needed
    Purchasing_PurchaseOrderDetail.drop(columns=['MergedID'], inplace=True, errors='ignore')

    # Drop rows where Source is 'NorthWind'
    if 'Source' in Purchasing_PurchaseOrderDetail.columns:
        Purchasing_PurchaseOrderDetail = Purchasing_PurchaseOrderDetail[Purchasing_PurchaseOrderDetail['Source'] != 'NorthWind']
        Purchasing_PurchaseOrderDetail.reset_index(drop=True, inplace=True)
        print("Rows with Source == 'NorthWind' have been removed.")
    else:
        print("The 'Source' column does not exist in Purchasing_PurchaseOrderDetail.")

    # Update the dictionary with the modified DataFrame
    dfs_sourcedatamodel["Purchasing_PurchaseOrderDetail"] = Purchasing_PurchaseOrderDetail

    # Print the result
    print(f"Rows: {len(Purchasing_PurchaseOrderDetail)}")
    print(Purchasing_PurchaseOrderDetail.head())

   
    print("The updated table has been saved to 'Purchasing_PurchaseOrderDetail.csv'.")
else:
    print("The table 'Purchasing_PurchaseOrderDetail' does not exist in the database.")


#table: Suppliers

#get dataframe from dictionary
Suppliers = df_NorthWind.get("Suppliers")


# Check if the DataFrame exists
if Suppliers is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Suppliers"] = Suppliers

    print(f" Rows: {len(Suppliers)}")
    print(Suppliers.head())
else:
    print("The table does not exist in database.")


#table: Person_Person
#Bronnen: Person_Person + Employees + Employee


# Laden van de datasets (Vervang dit met je eigen DataFrame-imports)
Person_Person_df = df_AdventureWorks.get("Person_Person")
Employees_df = df_NorthWind.get("Employees")
Employee_df = df_aenc.get("Employee")

# Voeg 'Source' kolom toe om de herkomst te behouden
Person_Person_df["Source"] = "AdventureWorks"
Employees_df["Source"] = "Northwind"
Employee_df["Source"] = "aenc"

# Hernoem kolommen zodat ze consistent zijn met Person_Person
Employees_df.rename(columns={"EmployeeID": "BusinessEntityID", "HomePhone": "PhoneNumber"}, inplace=True)
Employee_df.rename(columns={"emp_id": "BusinessEntityID", "phone": "PhoneNumber"}, inplace=True)

# Voeg een unieke sleutel toe (MergedBusinessEntityID) om overlaps te voorkomen
Person_Person_df["MergedBusinessEntityID"] = Person_Person_df["BusinessEntityID"]
Employees_df["MergedBusinessEntityID"] = Employees_df["BusinessEntityID"] + 100000  # Offset voor Northwind
Employee_df["MergedBusinessEntityID"] = Employee_df["BusinessEntityID"] + 200000  # Offset voor AENC

# Houd alleen de relevante kolommen
Person_Person_df = Person_Person_df[["MergedBusinessEntityID", "BusinessEntityID", "PersonType", "NameStyle", "Title", "LastName", "Suffix", "EmailPromotion", "rowguid",  "ModifiedDate", "Source"]]
Employees_df = Employees_df[["MergedBusinessEntityID", "BusinessEntityID", "Title", "LastName", "PhoneNumber", "Source"]]
Employee_df = Employee_df[["MergedBusinessEntityID", "BusinessEntityID", "emp_fname", "emp_lname", "PhoneNumber", "Source"]]

# Hernoemen van kolommen zodat ze overeenkomen
Employees_df.rename(columns={"Title": "Title", "LastName": "LastName"}, inplace=True)
Employee_df.rename(columns={"emp_fname": "Title", "emp_lname": "LastName"}, inplace=True)

# Samenvoegen van de tabellen zonder merge (alle data onder elkaar)
merged_person = pd.concat([Person_Person_df, Employees_df, Employee_df], ignore_index=True)

dfs_sourcedatamodel["Person_Person"] = merged_person


# Mapping maken voor toekomstige foreign key updates
person_mapping = merged_person[["BusinessEntityID", "Source", "MergedBusinessEntityID"]]
df_mapping["person_mapping"] = person_mapping

# Print het resultaat
print(merged_person)


#table: HumanResources_Department
#bronnen: HumanResources_Department + Department

# Laden van de datasets (vervang met echte DataFrame-imports)
HumanResources_Department_df = df_AdventureWorks.get("HumanResources_Department")
Department_df = df_aenc.get("Department")

# Voeg 'Source' kolom toe
HumanResources_Department_df["Source"] = "AdventureWorks"
Department_df["Source"] = "AENC"

# Hernoem kolommen zodat ze consistent zijn
Department_df.rename(columns={"dept_id": "DepartmentID", "dept_name": "Name"}, inplace=True)

# Houd alleen de relevante kolommen
HumanResources_Department_df = HumanResources_Department_df[["DepartmentID", "Name", "GroupName", "ModifiedDate", "Source"]]
Department_df = Department_df[["DepartmentID", "Name", "Source"]]

# Voeg een placeholder toe voor ontbrekende GroupName en ModifiedDate in AENC-data
Department_df["GroupName"] = None
Department_df["ModifiedDate"] = None

# Samenvoegen van de tabellen zonder merge (alle data onder elkaar)
merged_departments = pd.concat([HumanResources_Department_df, Department_df], ignore_index=True)

dfs_sourcedatamodel["HumanResources_Department"] = merged_departments

print(HumanResources_Department_df["GroupName"].str.len().max())  # Check maximale lengte 

# Print het resultaat
print(merged_departments)


#table: HumanResources_Employee
#Bronnen: HumanResources_Employee + Employees


# Laden van de datasets (Vervang dit met je eigen DataFrame-imports)
Person_Person_df = df_AdventureWorks.get("Person_Person")
Employees_df2 = df_NorthWind.get("Employees")
Employee_df2 = df_aenc.get("Employee")
HumanResources_Employee_df = df_AdventureWorks.get("HumanResources_Employee")  # Nieuwe tabel

# Voeg 'Source' kolom toe om de herkomst te behouden
Person_Person_df["Source"] = "AdventureWorks"
Employees_df2["Source"] = "NorthWind"
Employee_df2["Source"] = "AENC"

# Hernoem kolommen zodat ze consistent zijn met Person_Person
Employees_df2.rename(columns={"EmployeeID": "BusinessEntityID", "Title": "JobTitle", "BirthDate": "BirthDate", "HireDate": "HireDate", "TitleOfCourtesy": "Gender"}, inplace=True)
Employee_df2.rename(columns={"emp_id": "BusinessEntityID", "start_date": "HireDate", "birth_date": "BirthDate", "sex": "Gender", "salary": "salary", "manager_id": "ManagerID","dept_id":"DepartmentID"}, inplace=True)

# Voeg een unieke sleutel toe (MergedBusinessEntityID) om overlaps te voorkomen
Person_Person_df["MergedBusinessEntityID"] = Person_Person_df["BusinessEntityID"]
Employees_df2["MergedBusinessEntityID"] = Employees_df2["BusinessEntityID"] + 100000  # Offset voor Northwind
Employee_df2["MergedBusinessEntityID"] = Employee_df2["BusinessEntityID"] + 200000  # Offset voor AENC

# Voeg lege waarden toe voor ontbrekende kolommen
Employee_df2["JobTitle"] = None

# Mapping maken voor ManagerID verwijzing
manager_mapping = Employee_df2[["BusinessEntityID", "MergedBusinessEntityID"]].copy()
manager_mapping.rename(columns={"BusinessEntityID": "OldManagerID", "MergedBusinessEntityID": "NewManagerID"}, inplace=True)

# Houd alleen de relevante kolommen
Person_Person_df = Person_Person_df[["MergedBusinessEntityID", "BusinessEntityID", "Source"]]
Employees_df2 = Employees_df2[["MergedBusinessEntityID", "BusinessEntityID", "JobTitle", "BirthDate", "HireDate", "Gender", "Source"]]
Employee_df2 = Employee_df2[["MergedBusinessEntityID", "BusinessEntityID", "JobTitle", "BirthDate", "HireDate", "Gender", "salary", "ManagerID", "Source", "DepartmentID"]]

# Merge managerID correct op basis van de mapping
Employee_df2 = Employee_df2.merge(manager_mapping, how="left", left_on="ManagerID", right_on="OldManagerID")
Employee_df2.drop(columns=["OldManagerID", "ManagerID"], inplace=True)
Employee_df2.rename(columns={"NewManagerID": "ManagerID"}, inplace=True)

# Merge Person_Person met HumanResources_Employee op BusinessEntityID
Person_Person_df = Person_Person_df.merge(
    HumanResources_Employee_df[["BusinessEntityID", "DepartmentID", "BirthDate", "HireDate", "JobTitle", "Gender"]],
    on="BusinessEntityID",
    how="left"
)

# Samenvoegen van de tabellen zonder concat (één enkele tabel)
merged_human_resources_Employee = pd.concat([Person_Person_df, Employees_df2, Employee_df2], ignore_index=True)

# Pas de volgorde van de kolommen aan
column_order = ["MergedBusinessEntityID", "BusinessEntityID", "JobTitle", "BirthDate", "HireDate", "Gender", "salary", "ManagerID", "DepartmentID", "Source"]
merged_human_resources_Employee = merged_human_resources_Employee[column_order]

dfs_sourcedatamodel["HumanResources_Employee"] = merged_human_resources_Employee

# Mapping maken voor toekomstige foreign key updates
employee_mapping = merged_human_resources_Employee[["BusinessEntityID", "Source", "MergedBusinessEntityID"]]
df_mapping["employee_mapping"] = employee_mapping

# Print het resultaat
print(merged_human_resources_Employee)

merged_human_resources_Employee["salary"] = merged_human_resources_Employee["salary"].fillna(0).astype(float)




#Table: Person_Address

#get dataframe from dictionary
Person_Adress_Df = df_AdventureWorks.get("Person_Address")
column_order = ["AddressID", "AddressLine1", "AddressLine2", "City", "StateProvinceID", "PostalCode","BusinessEntityID"]
Person_Adress_Df = Person_Adress_Df[column_order]

# Check if the DataFrame exists
if Person_Adress_Df is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Person_Address"] = Person_Adress_Df

    print(f"Rows: {len(Person_Adress_Df)}")
    print(Person_Adress_Df.head())
else:
    print("The table does not exist in database.")


#table: Shippers
#bronnen: Shippers + Orders 

#get dataframe from dictionary
Shippers = df_NorthWind.get("Shippers")

# Check if the DataFrame exists
if Shippers is not None:
    # Add the DataFrame to the sourcedatamodel dictionary
    dfs_sourcedatamodel["Shippers"] = Shippers

    print(f" Rows: {len(Shippers)}")
    print(Shippers.head())
else:
    print("The table does not exist in database.")


#table: Sales_SalesOrderHeader
#Bronnen: Sales_SalesOrderHeader + Orders + Sales_Order

# ========== 1. Laad de datasets ==========
sales_adventureworks_df = df_AdventureWorks.get("Sales_SalesOrderHeader")
northwind_orders_df = df_NorthWind.get("Orders")
aenc_sales_order_df = df_aenc.get("Sales_Order")

# ========== 2. Voeg Source toe aan elke DataFrame ==========
sales_adventureworks_df["Source"] = "AdventureWorks"
northwind_orders_df["Source"] = "NorthWind"
aenc_sales_order_df["Source"] = "AENC"

# ========== 3. Hernoem kolommen voor consistentie ==========
northwind_orders_df.rename(columns={
    "OrderID": "SalesOrderID",
    "OrderDate": "OrderDate",
    "ShippedDate": "ShipDate",
    "CustomerID": "CustomerID",
    "EmployeeID": "SalesPersonID",
    "Freight": "SubTotal",
    "ShipVia": "ShipVia",
    "ShipCountry": "ShipCountry",
    "ShipRegion": "ShipRegion",
    "ShipCity": "ShipCity"
}, inplace=True)

aenc_sales_order_df.rename(columns={
    "id": "SalesOrderID",
    "order_date": "OrderDate",
    "cust_id": "CustomerID",
    "sales_rep": "SalesPersonID",
    "region": "ShipRegion"
}, inplace=True)

# ========== 4. Zorg dat CustomerID overal een string is ==========
sales_adventureworks_df["CustomerID"] = sales_adventureworks_df["CustomerID"].astype(str)
northwind_orders_df["CustomerID"] = northwind_orders_df["CustomerID"].astype(str)
aenc_sales_order_df["CustomerID"] = aenc_sales_order_df["CustomerID"].astype(str)

# Ook in de customer_mapping DataFrame
customer_mapping["CustomerID"] = customer_mapping["CustomerID"].astype(str)

# ========== 5. Ontbrekende kolommen aanvullen met None ==========
for df in [sales_adventureworks_df, northwind_orders_df, aenc_sales_order_df]:
    for col in ["RevisionNumber", "TerritoryID", "SubTotal", "DueDate",
                "ShipDate", "ShipVia", "ShipCountry", "ShipRegion", "ShipCity"]:
        if col not in df.columns:
            df[col] = None

# ========== 6. Selecteer en orden de relevante kolommen ==========
cols = [
    "SalesOrderID", "RevisionNumber", "OrderDate", "DueDate", "ShipDate", 
    "CustomerID", "SalesPersonID", "TerritoryID", "SubTotal", 
    "ShipVia", "ShipCountry", "ShipRegion", "ShipCity", "Source"
]

sales_adventureworks_df = sales_adventureworks_df[cols]
northwind_orders_df = northwind_orders_df[cols]
aenc_sales_order_df = aenc_sales_order_df[cols]

# ========== 7. Concateneer de drie DataFrames ==========
merged_sales_salesOrderHeader = pd.concat(
    [sales_adventureworks_df, northwind_orders_df, aenc_sales_order_df], 
    ignore_index=True
)

# ========== 8. CustomerID bijwerken via customer_mapping ==========
# Hier nemen we aan dat 'customer_mapping' al bestaat, met kolommen ["CustomerID", "Source", "MergedCustomerID"].
merged_sales_salesOrderHeader = merged_sales_salesOrderHeader.merge(
    customer_mapping,
    on=["CustomerID", "Source"],
    how="left"
)

# Vervang de CustomerID door MergedCustomerID
merged_sales_salesOrderHeader["CustomerID"] = merged_sales_salesOrderHeader["MergedCustomerID"]
merged_sales_salesOrderHeader.drop(columns=["MergedCustomerID"], inplace=True)

# ========== 9. SalesPersonID bijwerken via employee_mapping ==========
# Hier nemen we aan dat 'employee_mapping' al bestaat, met kolommen ["BusinessEntityID", "Source", "MergedBusinessEntityID"].
merged_sales_salesOrderHeader = merged_sales_salesOrderHeader.merge(
    employee_mapping,
    left_on=["SalesPersonID", "Source"],
    right_on=["BusinessEntityID", "Source"],
    how="left",
    suffixes=("", "_emp")
)

# Vervang SalesPersonID door de gemapte MergedBusinessEntityID
merged_sales_salesOrderHeader["SalesPersonID"] = merged_sales_salesOrderHeader["MergedBusinessEntityID"]
merged_sales_salesOrderHeader.drop(columns=["BusinessEntityID", "MergedBusinessEntityID"], inplace=True)

# ========== 10. Sla op in het data model en exporteer ==========
dfs_sourcedatamodel["Sales_SalesOrderHeader"] = merged_sales_salesOrderHeader
print(merged_sales_salesOrderHeader.head())

# Exporteer naar CSV (optioneel)



#Table: Sales_SalesOrderDetail
#Bronnen: Sales_SalesOrderDetail + OrderDetails + sales_order_item + product(aenc)


# ----- 1. Laad de datasets -----
Sales_SalesOrderDetail = df_AdventureWorks.get("Sales_SalesOrderDetail")
Sales_Order_Item = df_aenc.get("Sales_Order_Item")
OrderDetails = df_NorthWind.get("OrderDetails")
df_product2 = df_aenc.get("Product")  

df_product2.rename(columns={"id": "ProductID"}, inplace=True)

# ----- 2. Voeg Source toe aan elke DataFrame -----
Sales_SalesOrderDetail["Source"] = "AdventureWorks"
Sales_Order_Item["Source"] = "AENC"
OrderDetails["Source"] = "NorthWind"

# ----- 3. Verwerk de AdventureWorks data -----
# De AdventureWorks tabel bevat al de meeste gewenste kolommen
Sales_SalesOrderDetail = Sales_SalesOrderDetail.copy()

Sales_SalesOrderDetail = Sales_SalesOrderDetail[["SalesOrderID", "SalesOrderDetailID", "OrderQty", "ProductID", 
                 "UnitPrice", "UnitPriceDiscount", "LineTotal", "Source"]]

# ----- 4. Verwerk de AENC data -----
Sales_Order_Item = Sales_Order_Item.copy()
# Hernoem de kolommen: 
# id -> SalesOrderID, line_id -> SalesOrderDetailID, prod_id -> ProductID, quantity -> OrderQty
Sales_Order_Item.rename(columns={
    "id": "SalesOrderID",
    "line_id": "SalesOrderDetailID",
    "prod_id": "ProductID",
    "quantity": "OrderQty"
}, inplace=True)
# Zorg dat OrderQty als string is
# Voor AENC ontbreekt UnitPrice en UnitPriceDiscount:
# Probeer de unit_price te halen uit een extra DataFrame (df_product2) uit de AENC-bron
if  df_product2 is not None:
    # Zorg dat in df_product2 ook de kolom "ProductID" voorkomt; dan mergen we de unit_price
    Sales_Order_Item = Sales_Order_Item.merge(df_product2[["ProductID", "unit_price"]], on="ProductID", how="left")
    Sales_Order_Item.rename(columns={"unit_price": "UnitPrice"}, inplace=True)
else:
   print("The table does not exist in database.")

# ----- 5. Verwerk de NorthWind data -----
OrderDetails = OrderDetails.copy()
# Hernoem de kolommen: OrderID -> SalesOrderID, Quantity -> OrderQty, Discount -> UnitPriceDiscount
OrderDetails.rename(columns={
    "OrderID": "SalesOrderID",
    "Quantity": "OrderQty",
    "Discount": "UnitPriceDiscount"
}, inplace=True)
# Creëer een SalesOrderDetailID omdat deze kolom ontbreekt: gebruik per SalesOrderID een cumulatieve telling
OrderDetails["SalesOrderDetailID"] = OrderDetails.groupby("SalesOrderID").cumcount() + 1

OrderDetails["LineTotal"] = None
OrderDetails = OrderDetails[["SalesOrderID", "SalesOrderDetailID", "OrderQty", "ProductID", 
               "UnitPrice", "UnitPriceDiscount", "LineTotal", "Source"]]

# ----- 6. Combineer de drie bronnen -----
merged_orderdetail = pd.concat([Sales_SalesOrderDetail, Sales_Order_Item, OrderDetails], ignore_index=True)

# ----- 7. Werk de ProductID bij via product_mapping -----
# Verwacht dat product_mapping een DataFrame is met kolommen: ["ProductID", "Source", "MergedID"]
# Merge de mapping op ProductID en Source en vervang de originele ProductID door de MergedID
merged_orderdetail = merged_orderdetail.merge(
    product_mapping,
    on=["ProductID", "Source"],
    how="left"
)
merged_orderdetail["ProductID"] = merged_orderdetail["MergedID"]
merged_orderdetail.drop(columns=["MergedID"], inplace=True)


# ----- 1. Laad de datasets -----
Sales_SalesOrderDetail = df_AdventureWorks.get("Sales_SalesOrderDetail")
Sales_Order_Item = df_aenc.get("Sales_Order_Item")
OrderDetails = df_NorthWind.get("OrderDetails")
df_product2 = df_aenc.get("Product")  

df_product2.rename(columns={"id": "ProductID"}, inplace=True)

# ----- 2. Voeg Source toe aan elke DataFrame -----
Sales_SalesOrderDetail["Source"] = "AdventureWorks"
Sales_Order_Item["Source"] = "AENC"
OrderDetails["Source"] = "NorthWind"

# ----- 3. Verwerk de AdventureWorks data -----
# De AdventureWorks tabel bevat al de meeste gewenste kolommen
Sales_SalesOrderDetail = Sales_SalesOrderDetail.copy()

Sales_SalesOrderDetail = Sales_SalesOrderDetail[["SalesOrderID", "SalesOrderDetailID", "OrderQty", "ProductID", 
                 "UnitPrice", "UnitPriceDiscount", "LineTotal", "Source"]]

# ----- 4. Verwerk de AENC data -----
Sales_Order_Item = Sales_Order_Item.copy()
# Hernoem de kolommen: 
# id -> SalesOrderID, line_id -> SalesOrderDetailID, prod_id -> ProductID, quantity -> OrderQty
Sales_Order_Item.rename(columns={
    "id": "SalesOrderID",
    "line_id": "SalesOrderDetailID",
    "prod_id": "ProductID",
    "quantity": "OrderQty"
}, inplace=True)
# Zorg dat OrderQty als string is
# Voor AENC ontbreekt UnitPrice en UnitPriceDiscount:
# Probeer de unit_price te halen uit een extra DataFrame (df_product2) uit de AENC-bron
if  df_product2 is not None:
    # Zorg dat in df_product2 ook de kolom "ProductID" voorkomt; dan mergen we de unit_price
    Sales_Order_Item = Sales_Order_Item.merge(df_product2[["ProductID", "unit_price"]], on="ProductID", how="left")
    Sales_Order_Item.rename(columns={"unit_price": "UnitPrice"}, inplace=True)
else:
   print("The table does not exist in database.")

# ----- 5. Verwerk de NorthWind data -----
OrderDetails = OrderDetails.copy()
# Hernoem de kolommen: OrderID -> SalesOrderID, Quantity -> OrderQty, Discount -> UnitPriceDiscount
OrderDetails.rename(columns={
    "OrderID": "SalesOrderID",
    "Quantity": "OrderQty",
    "Discount": "UnitPriceDiscount"
}, inplace=True)
# Creëer een SalesOrderDetailID omdat deze kolom ontbreekt: gebruik per SalesOrderID een cumulatieve telling
OrderDetails["SalesOrderDetailID"] = OrderDetails.groupby("SalesOrderID").cumcount() + 1

OrderDetails["LineTotal"] = None
OrderDetails = OrderDetails[["SalesOrderID", "SalesOrderDetailID", "OrderQty", "ProductID", 
               "UnitPrice", "UnitPriceDiscount", "LineTotal", "Source"]]

# ----- 6. Combineer de drie bronnen -----
merged_orderdetail = pd.concat([Sales_SalesOrderDetail, Sales_Order_Item, OrderDetails], ignore_index=True)

# ----- 7. Werk de ProductID bij via product_mapping -----
# Verwacht dat product_mapping een DataFrame is met kolommen: ["ProductID", "Source", "MergedID"]
# Merge de mapping op ProductID en Source en vervang de originele ProductID door de MergedID
merged_orderdetail = merged_orderdetail.merge(
    product_mapping,
    on=["ProductID", "Source"],
    how="left"
)
merged_orderdetail["ProductID"] = merged_orderdetail["MergedID"]
merged_orderdetail.drop(columns=["MergedID"], inplace=True)

# Verwijder de kolom ShipDate als deze bestaat
if "ship_date" in merged_orderdetail.columns:
    merged_orderdetail = merged_orderdetail.drop(columns=["ship_date"])

# ----- 8. Sla het resultaat op in het datamodel -----
dfs_sourcedatamodel["Sales_SalesOrderDetail"] = merged_orderdetail

# Print het resultaat
print(merged_orderdetail.head())


# ----- 8. Sla het resultaat op in het datamodel -----
dfs_sourcedatamodel["Sales_SalesOrderDetail"] = merged_orderdetail

# Print het resultaat
print(merged_orderdetail.head())


#Vullen SDM


for tabel in dfs_sourcedatamodel:
    print(f"{tabel}: {dfs_sourcedatamodel[tabel].shape}")


#Connectie leggen SDM 


# Verbindingsgegevens
server = '127.0.0.1'        
port = '1433'               
database2 = 'SDMProject'         
username = 'SA'             
password = 'iDTyjZx7dRL4'  

# Connection string
connection_string2 = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server},{port};"
    f"DATABASE={database2};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
    "Timeout=30;"
)

# Maak verbinding met de database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Haal alle tabellen op``
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
tables = [row.TABLE_NAME for row in cursor.fetchall()]


# Sluit de verbinding
conn.close()





#Converteer datatypen met datums naar DATETIME


def convert_all_dates(dw):
    date_keywords = ["date", "time"]  # Zoek naar deze woorden in kolomnamen
    
    for table_name, df in dw.items():
        for col in df.columns:
            if any(keyword in col.lower() for keyword in date_keywords):  # Check op 'date' of 'time'
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')  # Converteer naar datetime
                    print(f"✅ Geconverteerd: {table_name}.{col}")
                except Exception as e:
                    print(f"⚠️ Fout bij converteren van {col} in {table_name}: {e}")
    
    return dw

# Pas de functie toe op je DataFrame-collectie
dfs_sourcedatamodel = convert_all_dates(dfs_sourcedatamodel)


#Vullen van database

def clean_nan_values(dw):
    for table_name, df in dw.items():
        # Zet alle NaN naar None zodat SQL Server NULL kan verwerken
        dw[table_name] = df.astype(object).where(pd.notnull(df), None)
    return dw

# Pas toe op je dataWarehouse dictionary
dfs_sourcedatamodel = clean_nan_values(dfs_sourcedatamodel)


def upload_dataframes_to_sql(dw):
    try:
        with pyodbc.connect(connection_string2, autocommit=False) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True  # Maakt batch-inserts sneller
            
            print("⏳ Uitschakelen van FOREIGN KEY constraints...")
            cursor.execute("EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'")
            conn.commit()
            
            # Loop over elke tabel
            for table_name, df in dw.items():
                print(f"\nBezig met uploaden van tabel: {table_name}...")
                
                columns = ', '.join([f'[{col}]' for col in df.columns])
                placeholders = ', '.join(['?'] * len(df.columns))
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                # Gebruik batch commit
                batch_size = 1000
                data_list = [tuple(row) for _, row in df.iterrows()]
                
                for i in range(0, len(data_list), batch_size):
                    try:
                        cursor.executemany(insert_query, data_list[i:i+batch_size])
                        conn.commit()
                        print(f"  ✅ Batch {i//batch_size + 1} geüpload ({len(data_list[i:i+batch_size])} rijen)")
                    except pyodbc.Error as e:
                        conn.rollback()
                        print(f"  ❌ Fout in batch {i//batch_size + 1}: {str(e)}")
                
            print("\n⏳ Herinschakelen van FOREIGN KEY constraints...")
            cursor.execute("EXEC sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'")
            conn.commit()
            
            print("\n🎉 Upload voltooid voor alle tabellen!")
            cursor.close()
            
    except pyodbc.Error as e:
        print(f"❌ Databasefout: {e}")

upload_dataframes_to_sql(dfs_sourcedatamodel)



