from http.client import HTTPException
from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

# Replace with your own connection details
server = os.getenv("SERVER")
database = os.getenv("DATABASE")
username = os.getenv("USER_NAME")
password = os.getenv("PASSWORD")
driver = os.getenv("DRIVER")

connection_string = f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"

app = FastAPI()

# Pydantic model for the Employee
class Employee(BaseModel):
    EmployeeID: int
    FirstName: str
    LastName: str
    BirthDate: str
    HireDate: str
    
class EmployeeUpdate(BaseModel):
    FirstName: Optional[str] = None
    LastName: Optional[str] = None
    BirthDate: Optional[str] = None
    HireDate: Optional[str] = None

# Function to get database connection
def get_db_connection():
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

@app.get("/employees/{employee_id}", response_model=Employee)
async def get_employee(employee_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT EmployeeID, FirstName, LastName, BirthDate, HireDate FROM Employees WHERE EmployeeID = ?", employee_id)
    row = cursor.fetchone()
    
    if row:
        employee = Employee(
            EmployeeID=row.EmployeeID,
            FirstName=row.FirstName,
            LastName=row.LastName,
            BirthDate=row.BirthDate.strftime('%Y-%m-%d'),
            HireDate=row.HireDate.strftime('%Y-%m-%d')
        )
        conn.close()
        return employee
    else:
        conn.close()
        raise HTTPException(status_code=404, detail="Employee not found")
    
@app.post("/employees/", response_model=Employee)
async def create_employee(employee: Employee):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            "INSERT INTO Employees (EmployeeID, FirstName, LastName, BirthDate, HireDate) VALUES (?, ?, ?, ?, ?)",
            employee.EmployeeID, employee.FirstName, employee.LastName, employee.BirthDate, employee.HireDate
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Error inserting into database: {e}")
    
    conn.close()
    return employee

@app.put("/employees/", response_model=Employee)
async def update_employee(employee: Employee):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE Employees
            SET FirstName = ?, LastName = ?, BirthDate = ?, HireDate = ?
            WHERE EmployeeID = ?
            """,
            employee.FirstName, employee.LastName, employee.BirthDate, employee.HireDate, employee.EmployeeID
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Error updating database: {e}")
    
    conn.close()
    return employee

@app.patch("/employees/{employee_id}", response_model=Employee)
async def patch_employee(employee_id: int, employee_update: EmployeeUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Prepare the SQL update statement dynamically
    update_fields = []
    update_values = []
    if employee_update.FirstName is not None:
        update_fields.append("FirstName = ?")
        update_values.append(employee_update.FirstName)
    if employee_update.LastName is not None:
        update_fields.append("LastName = ?")
        update_values.append(employee_update.LastName)
    if employee_update.BirthDate is not None:
        update_fields.append("BirthDate = ?")
        update_values.append(employee_update.BirthDate)
    if employee_update.HireDate is not None:
        update_fields.append("HireDate = ?")
        update_values.append(employee_update.HireDate)
    
    update_values.append(employee_id)
    
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    update_statement = f"UPDATE Employees SET {', '.join(update_fields)} WHERE EmployeeID = ?"

    try:
        cursor.execute(update_statement, *update_values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Error updating database: {e}")
    
    # Fetch the updated employee record to return
    cursor.execute("SELECT EmployeeID, FirstName, LastName, BirthDate, HireDate FROM Employees WHERE EmployeeID = ?", employee_id)
    row = cursor.fetchone()
    
    if row:
        employee = Employee(
            EmployeeID=row.EmployeeID,
            FirstName=row.FirstName,
            LastName=row.LastName,
            BirthDate=row.BirthDate.strftime('%Y-%m-%d'),
            HireDate=row.HireDate.strftime('%Y-%m-%d')
        )
        conn.close()
        return employee
    else:
        conn.close()
        raise HTTPException(status_code=404, detail="Employee not found")

@app.delete("/employees/{employee_id}", response_model=Employee)
async def delete_employee(employee_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Fetch the employee record to return after deletion
    cursor.execute("SELECT EmployeeID, FirstName, LastName, BirthDate, HireDate FROM Employees WHERE EmployeeID = ?", employee_id)
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Employee not found")
    
    employee = Employee(
        EmployeeID=row.EmployeeID,
        FirstName=row.FirstName,
        LastName=row.LastName,
        BirthDate=row.BirthDate.strftime('%Y-%m-%d'),
        HireDate=row.HireDate.strftime('%Y-%m-%d')
    )
    
    try:
        cursor.execute("DELETE FROM Employees WHERE EmployeeID = ?", employee_id)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Error deleting from database: {e}")
    
    conn.close()
    return employee

