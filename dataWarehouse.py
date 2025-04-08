
import pandas as pd
import sqlite3
import warnings
import pyodbc
import numpy as np
import os

warnings.simplefilter('ignore')
print(pyodbc.drivers())


#verbinding maken met SourceDataModel, en inlezen tabellen

# Verbindingsgegevens
server = '127.0.0.1'        
port = '1433'               
database = 'SDMProject'         
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
sdm_dfs = {}

# Loop door alle tabellen en laad ze in Pandas DataFrames
for table in tables:
    query = f"SELECT * FROM [{table}]"
    df = pd.read_sql(query, conn)
    sdm_dfs[table] = df
    print(f"Tabel '{table}' ingelezen met {df.shape[0]} rijen en {df.shape[1]} kolommen.")

# Sluit de verbinding
conn.close()

# Print de kolomnamen en de eerste paar rijen van elke DataFrame
for table_name, df in sdm_dfs.items():
    print(f"\nTable: {table_name}")
    print("Columns:", df.columns.tolist())
    print(df.head())
    




#dictionary maken voor goede dataframes

DataWarehouse_dict = {
}


#HumanRescources_department


df_HumanRescources_Department = sdm_dfs['HumanResources_Department'].copy()

df_HumanRescources_Department.drop(columns=['ModifiedDate'], inplace=True)

DataWarehouse_dict['HumanResources_Department'] = df_HumanRescources_Department

df_HumanRescources_Department.head()


#Person_Person
# Kopieer de SDM-tabel
df_Person_Person = sdm_dfs['Person_Person'].copy()

# Drop eerst de originele BusinessEntityID
df_Person_Person = df_Person_Person.drop(columns=["BusinessEntityID"])

# Hernoem MergedBusinessEntityID naar BusinessEntityID
df_Person_Person = df_Person_Person.rename(columns={
    "MergedBusinessEntityID": "BusinessEntityID"
})

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_person = df_Person_Person[["BusinessEntityID", "Title", "LastName", "Source"]].copy()

# Truncate strings naar max lengte
df_dwh_person["Title"] = df_dwh_person["Title"].astype(str).str.slice(0, 50)
df_dwh_person["LastName"] = df_dwh_person["LastName"].astype(str).str.slice(0, 255)
df_dwh_person["Source"] = df_dwh_person["Source"].astype(str).str.slice(0, 50)

DataWarehouse_dict['Person_Person'] = df_dwh_person

# Preview
print(df_dwh_person.head())



#HumanResources_Employee
# Kopieer de SDM-tabel
df_HumanResources_Employee = sdm_dfs['HumanResources_Employee'].copy()

# Drop eerst de originele BusinessEntityID
df_HumanResources_Employee = df_HumanResources_Employee.drop(columns=["BusinessEntityID"])

# Hernoem MergedBusinessEntityID naar BusinessEntityID
df_HumanResources_Employee = df_HumanResources_Employee.rename(columns={
    "MergedBusinessEntityID": "BusinessEntityID"
})

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_employee = df_HumanResources_Employee[["BusinessEntityID", "JobTitle", "Salary", "DepartmentID", "ManagerID", "Source"]].copy()

# Truncate strings naar max lengte
df_dwh_employee["JobTitle"] = df_dwh_employee["JobTitle"].astype(str).str.slice(0, 100)
df_dwh_employee["Source"] = df_dwh_employee["Source"].astype(str).str.slice(0, 50)

# Opslaan in het DataWarehouse
DataWarehouse_dict['HumanResources_Employee'] = df_dwh_employee

# Preview
print(df_dwh_employee.head())


#Person_Address
# Kopieer de SDM-tabel
df_Person_Address = sdm_dfs['Person_Address'].copy()

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_address = df_Person_Address[["AddressID", "City", "BusinessEntityID"]].copy()

# Truncate strings naar maximale lengte
df_dwh_address["City"] = df_dwh_address["City"].astype(str).str.slice(0, 100)

# Opslaan in het DataWarehouse
DataWarehouse_dict['Person_Address'] = df_dwh_address

# Preview
print(df_dwh_address.head())


#Production_ProductCategory

# Kopieer de SDM-tabel
df_Production_ProductCategory = sdm_dfs['Production_ProductCategory'].copy()

# Drop eerst de originele ProductCategoryID
df_Production_ProductCategory = df_Production_ProductCategory.drop(columns=["ProductCategoryID"])

# Hernoem MergedCategoryID naar ProductCategoryID
df_Production_ProductCategory = df_Production_ProductCategory.rename(columns={
    "MergedCategoryID": "ProductCategoryID"
})

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_product_category = df_Production_ProductCategory[["ProductCategoryID", "Name", "Source"]].copy()

# Truncate strings naar maximale lengte
df_dwh_product_category["Name"] = df_dwh_product_category["Name"].astype(str).str.slice(0, 100)
df_dwh_product_category["Source"] = df_dwh_product_category["Source"].astype(str).str.slice(0, 50)

# Opslaan in het DataWarehouse
DataWarehouse_dict['Production_ProductCategory'] = df_dwh_product_category

# Preview
print(df_dwh_product_category.head())


#Shippers

# Kopieer de SDM-tabel
df_Shippers = sdm_dfs['Shippers'].copy()

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_shippers = df_Shippers[["ShipperID", "CompanyName"]].copy()

# Truncate strings naar maximale lengte
df_dwh_shippers["CompanyName"] = df_dwh_shippers["CompanyName"].astype(str).str.slice(0, 100)

# Opslaan in het DataWarehouse
DataWarehouse_dict['Shippers'] = df_dwh_shippers

# Preview
print(df_dwh_shippers.head())


#Suppliers
# Kopieer de SDM-tabel
df_Suppliers = sdm_dfs['Suppliers'].copy()

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_suppliers = df_Suppliers[["SupplierID", "CompanyName", "City", "Country", "Region"]].copy()

# Truncate strings naar maximale lengte
df_dwh_suppliers["CompanyName"] = df_dwh_suppliers["CompanyName"].astype(str).str.slice(0, 100)
df_dwh_suppliers["City"] = df_dwh_suppliers["City"].astype(str).str.slice(0, 100)
df_dwh_suppliers["Country"] = df_dwh_suppliers["Country"].astype(str).str.slice(0, 100)
df_dwh_suppliers["Region"] = df_dwh_suppliers["Region"].astype(str).str.slice(0, 100)

# Opslaan in het DataWarehouse
DataWarehouse_dict['Suppliers'] = df_dwh_suppliers

# Preview
print(df_dwh_suppliers.head())


# <h3>Production_Product

# Kopieer de SDM-tabel
df_Production_Product = sdm_dfs['Production_Product'].copy()

# Drop de originele ProductID
df_Production_Product = df_Production_Product.drop(columns=["ProductID"])

# Hernoem MergedID naar ProductID
df_Production_Product = df_Production_Product.rename(columns={
    "MergedID": "ProductID"
})

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_product = df_Production_Product[[
    "ProductID", "Name", "StandardCost", "ProductCategoryID", "QuantityPerUnit",
    "DaysToManufacture", "ProductLine", "ListPrice", "UnitsInStock", "UnitsOnOrder",
    "SellStartDate", "SellEndDate",  "SupplierID"
]].copy()

# Truncate strings naar maximale lengte
df_dwh_product["Name"] = df_dwh_product["Name"].astype(str).str.slice(0, 100)
df_dwh_product["QuantityPerUnit"] = df_dwh_product["QuantityPerUnit"].astype(str).str.slice(0, 50)
df_dwh_product["ProductLine"] = df_dwh_product["ProductLine"].astype(str).str.slice(0, 30)

# Converteer kolommen naar het juiste datatyp
df_dwh_product["StandardCost"] = pd.to_numeric(df_dwh_product["StandardCost"], errors='coerce')
df_dwh_product["ListPrice"] = pd.to_numeric(df_dwh_product["ListPrice"], errors='coerce')
df_dwh_product["UnitsInStock"] = pd.to_numeric(df_dwh_product["UnitsInStock"], errors='coerce')
df_dwh_product["UnitsOnOrder"] = pd.to_numeric(df_dwh_product["UnitsOnOrder"], errors='coerce')
df_dwh_product["DaysToManufacture"] = pd.to_numeric(df_dwh_product["DaysToManufacture"], errors='coerce')
df_dwh_product["SellStartDate"] = pd.to_datetime(df_dwh_product["SellStartDate"], errors='coerce')
df_dwh_product["SellEndDate"] = pd.to_datetime(df_dwh_product["SellEndDate"], errors='coerce')

# Opslaan in het DataWarehouse
DataWarehouse_dict['Production_Product'] = df_dwh_product

# Preview
print(df_dwh_product.head())


#Purchasing_Vendor
# Kopieer de SDM-tabel
df_Purchasing_Vendor = sdm_dfs['Purchasing_Vendor'].copy()

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_vendor = df_Purchasing_Vendor[["BusinessEntityID", "Name"]].copy()

# Truncate strings naar maximale lengte
df_dwh_vendor["Name"] = df_dwh_vendor["Name"].astype(str).str.slice(0, 255)

# Opslaan in het DataWarehouse
DataWarehouse_dict['Purchasing_Vendor'] = df_dwh_vendor

# Preview
print(df_dwh_vendor.head())


#Fact_Purchase_Order
# Kopieer de SDM-tabellen
df_PurchaseOrderHeader = sdm_dfs['Purchasing_PurchaseOrderHeader'].copy()
df_PurchaseOrderDetail = sdm_dfs['Purchasing_PurchaseOrderDetail'].copy()

# Merge de twee tabellen op PurchaseOrderID
df_fact_purchase_order = pd.merge(
    df_PurchaseOrderHeader,
    df_PurchaseOrderDetail,
    on="PurchaseOrderID",
    how="inner"
)

# Selecteer en hernoem de relevante kolommen
df_fact_purchase_order = df_fact_purchase_order[[
    "PurchaseOrderID",
    "PurchaseOrderDetailID",
    "OrderDate",  # Hernoemen naar PurchaseDate
    "ShipDate",
    "EmployeeID",
    "ProductID",
    "VendorID",
    "OrderQty",
    "UnitPrice",
    "ReceivedQty",
    "RejectedQty",
    "StockedQty",
    "SubTotal",
    "TotalDue",
    "Source"
]].rename(columns={
    "OrderDate": "PurchaseDate"
})

# Converteer kolommen naar het juiste datatype
df_fact_purchase_order["PurchaseDate"] = pd.to_datetime(df_fact_purchase_order["PurchaseDate"], errors="coerce")
df_fact_purchase_order["ShipDate"] = pd.to_datetime(df_fact_purchase_order["ShipDate"], errors="coerce")
df_fact_purchase_order["OrderQty"] = pd.to_numeric(df_fact_purchase_order["OrderQty"], errors="coerce")
df_fact_purchase_order["UnitPrice"] = pd.to_numeric(df_fact_purchase_order["UnitPrice"], errors="coerce")
df_fact_purchase_order["ReceivedQty"] = pd.to_numeric(df_fact_purchase_order["ReceivedQty"], errors="coerce")
df_fact_purchase_order["RejectedQty"] = pd.to_numeric(df_fact_purchase_order["RejectedQty"], errors="coerce")
df_fact_purchase_order["StockedQty"] = pd.to_numeric(df_fact_purchase_order["StockedQty"], errors="coerce")
df_fact_purchase_order["SubTotal"] = pd.to_numeric(df_fact_purchase_order["SubTotal"], errors="coerce")
df_fact_purchase_order["TotalDue"] = pd.to_numeric(df_fact_purchase_order["TotalDue"], errors="coerce")

# Opslaan in het DataWarehouse
DataWarehouse_dict['Fact_Purchase_Order'] = df_fact_purchase_order

# Preview
print(df_fact_purchase_order.head())


#Sales_SalesTerritory
# Kopieer de SDM-tabel
df_Sales_SalesTerritory = sdm_dfs['Sales_SalesTerritory'].copy()

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_sales_territory = df_Sales_SalesTerritory[[
    "TerritoryID", "Name", "region", "Group1", "SalesYTD", "SalesLastYear"
]].copy()

# Hernoem 'region' naar 'Region' om consistent te zijn met de SQL-tabel
df_dwh_sales_territory = df_dwh_sales_territory.rename(columns={"region": "Region"})

# Truncate strings naar maximale lengte
df_dwh_sales_territory["Name"] = df_dwh_sales_territory["Name"].astype(str).str.slice(0, 100)
df_dwh_sales_territory["Region"] = df_dwh_sales_territory["Region"].astype(str).str.slice(0, 100)
df_dwh_sales_territory["Group1"] = df_dwh_sales_territory["Group1"].astype(str).str.slice(0, 100)

# Converteer numerieke kolommen naar het juiste datatype
df_dwh_sales_territory["SalesYTD"] = pd.to_numeric(df_dwh_sales_territory["SalesYTD"], errors="coerce")
df_dwh_sales_territory["SalesLastYear"] = pd.to_numeric(df_dwh_sales_territory["SalesLastYear"], errors="coerce")

# Opslaan in het DataWarehouse
DataWarehouse_dict['Sales_SalesTerritory'] = df_dwh_sales_territory

# Preview
print(df_dwh_sales_territory.head())


#Sales_Store
# Kopieer de SDM-tabel
df_Sales_Store = sdm_dfs['Sales_Store'].copy()

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_sales_store = df_Sales_Store[["BusinessEntityID", "Name", "SalesPersonID"]].copy()

# Truncate strings naar maximale lengte
df_dwh_sales_store["Name"] = df_dwh_sales_store["Name"].astype(str).str.slice(0, 255)

# Opslaan in het DataWarehouse
DataWarehouse_dict['Sales_Store'] = df_dwh_sales_store

# Preview
print(df_dwh_sales_store.head())


#Sales_Customer

# Kopieer de SDM-tabel
df_Sales_Customer = sdm_dfs['Sales_Customer'].copy()

# Selecteer alleen de kolommen die je nodig hebt
df_dwh_sales_customer = df_Sales_Customer[[
    "MergedCustomerID", "PersonID", "TerritoryID", "CompanyName", "AccountNumber", "Source"
]].copy()

df_dwh_sales_customer = df_dwh_sales_customer.rename(columns={"PersonID": "SalesPersonID"})
df_dwh_sales_customer = df_dwh_sales_customer.rename(columns={"MergedCustomerID": "CustomerID"})


# Truncate strings naar maximale lengte
df_dwh_sales_customer["CompanyName"] = df_dwh_sales_customer["CompanyName"].astype(str).str.slice(0, 255)
df_dwh_sales_customer["AccountNumber"] = df_dwh_sales_customer["AccountNumber"].astype(str).str.slice(0, 50)
df_dwh_sales_customer["Source"] = df_dwh_sales_customer["Source"].astype(str).str.slice(0, 50)

# Opslaan in het DataWarehouse
DataWarehouse_dict['Sales_Customer'] = df_dwh_sales_customer

# Preview
print(df_dwh_sales_customer.head())


#Fact_Sales_Order
# Kopieer de SDM-tabellen
df_SalesOrderHeader = sdm_dfs['Sales_SalesOrderHeader'].copy()
df_SalesOrderDetail = sdm_dfs['Sales_SalesOrderDetail'].copy()

# Merge de twee tabellen op SalesOrderID
df_fact_sales_order = pd.merge(
    df_SalesOrderHeader,
    df_SalesOrderDetail,
    on="SalesOrderID",
    how="inner",
    suffixes=('_header', '_detail')  # Voorkom conflicten door suffixes toe te voegen
)

# Controleer welke kolommen aanwezig zijn na de merge
print("Kolommen na merge:", df_fact_sales_order.columns)

# Selecteer en hernoem de relevante kolommen
df_fact_sales_order = df_fact_sales_order[[
    "SalesOrderID",
    "SalesOrderDetailID",
    "OrderQty",
    "OrderDate",
    "ProductID",
    "CustomerID",
    "UnitPrice",
    "UnitPriceDiscount",
    "LineTotal",
    "SalesPersonID",
    "SubTotal",
    "ShipVia",
    "Source_header"  # Gebruik de juiste kolomnaam na de merge
]].rename(columns={
    "Source_header": "Source"  # Hernoem de kolom naar 'Source'
})

# Voeg een nieuwe kolom toe voor LineTotalDiscounted
df_fact_sales_order["LineTotalDiscounted"] = (
    pd.to_numeric(df_fact_sales_order["LineTotal"], errors="coerce") -
    (pd.to_numeric(df_fact_sales_order["UnitPriceDiscount"], errors="coerce") * pd.to_numeric(df_fact_sales_order["OrderQty"], errors="coerce"))
)

# Converteer kolommen naar het juiste datatype
df_fact_sales_order["OrderDate"] = pd.to_datetime(df_fact_sales_order["OrderDate"], errors="coerce")
df_fact_sales_order["OrderQty"] = pd.to_numeric(df_fact_sales_order["OrderQty"], errors="coerce")
df_fact_sales_order["UnitPrice"] = pd.to_numeric(df_fact_sales_order["UnitPrice"], errors="coerce")
df_fact_sales_order["UnitPriceDiscount"] = pd.to_numeric(df_fact_sales_order["UnitPriceDiscount"], errors="coerce")
df_fact_sales_order["LineTotal"] = pd.to_numeric(df_fact_sales_order["LineTotal"], errors="coerce")
df_fact_sales_order["SubTotal"] = pd.to_numeric(df_fact_sales_order["SubTotal"], errors="coerce")
df_fact_sales_order["ShipVia"] = pd.to_numeric(df_fact_sales_order["ShipVia"], errors="coerce")

# Opslaan in het DataWarehouse
DataWarehouse_dict['Fact_Sales_Order'] = df_fact_sales_order

# Preview
print(df_fact_sales_order.head())


#Region + territories + employeeterritories

df_dwh_region = sdm_dfs['Region'].copy()

DataWarehouse_dict['Region'] = df_dwh_region

# Preview
print(df_dwh_region.head())


df_dwh_territories = sdm_dfs['Territories'].copy()
DataWarehouse_dict['Territories'] = df_dwh_territories

# Preview
print(df_dwh_territories.head())


df_dwh_employeeterritories = sdm_dfs['EmployeeTerritories'].copy()
DataWarehouse_dict['EmployeeTerritories'] = df_dwh_employeeterritories
# Preview
print(df_dwh_employeeterritories.head())


#Bonus

df_dwh_Bonus = sdm_dfs['Bonus'].copy()
DataWarehouse_dict['Bonus'] = df_dwh_Bonus
# Preview   
print(df_dwh_Bonus.head())


#Vullen Datawarehouse


for tabel in DataWarehouse_dict:
    print(f"{tabel}: {DataWarehouse_dict[tabel].shape}")


# Verbindingsgegevens
server = '127.0.0.1'        
port = '1433'               
database3 = 'ProjectDataWarehouse'         
username = 'SA'             
password = 'iDTyjZx7dRL4'  

# Connection string
connection_string3 = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server},{port};"
    f"DATABASE={database3};"
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


# Sluit de verbinding
conn.close()


def clean_nan_values(dw):
    for table_name, df in dw.items():
        # Zet alle NaN naar None zodat SQL Server NULL kan verwerken
        dw[table_name] = df.astype(object).where(pd.notnull(df), None)
    return dw

# Pas toe op je dataWarehouse dictionary
DataWarehouse_dict = clean_nan_values(DataWarehouse_dict)


def upload_dataframes_to_sql(dw):
    try:
        with pyodbc.connect(connection_string3, autocommit=False) as conn:
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

upload_dataframes_to_sql(DataWarehouse_dict)



