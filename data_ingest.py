import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

# Database connection string
server = os.getenv("SERVER")
database = os.getenv("DATABASE")
username = os.getenv("USER_NAME")
password = os.getenv("PASSWORD")
driver = os.getenv("DRIVER")


connection_string = f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"


# Function to get database connection
def get_db_connection():
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

# Function to ingest CSV data into the Employees table
def ingest_csv_to_db(csv_file):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Ensure the dataframe columns match the table columns
        required_columns = ['EmployeeID', 'FirstName', 'LastName', 'BirthDate', 'HireDate']
        if not all(column in df.columns for column in required_columns):
            raise ValueError("CSV file does not contain the required columns")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for index, row in df.iterrows():
            try:
                cursor.execute(
                    "INSERT INTO Employees (EmployeeID, FirstName, LastName, BirthDate, HireDate) VALUES (?, ?, ?, ?, ?)",
                    row['EmployeeID'], row['FirstName'], row['LastName'], row['BirthDate'], row['HireDate']
                )
            except Exception as e:
                print(f"Error inserting row {index} into database: {e}")
        
        conn.commit()
        conn.close()
        print("Data ingestion completed successfully.")
    
    except Exception as e:
        print(f"Error ingesting CSV data: {e}")

# Path to the CSV file
csv_file = 'MOCK_DATA.csv'

# Ingest CSV data into the database
ingest_csv_to_db(csv_file)
