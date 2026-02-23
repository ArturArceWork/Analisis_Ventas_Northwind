from src.database import get_engine
import pandas as pd

def get_sales_by_category():
    """Retorna un DataFrame con las ventas totales por categoría."""
    engine = get_engine()
    query = """
    SELECT 
        c.CategoryName,
        SUM(od.UnitPrice * od.Quantity) as TotalRevenue
    FROM Categories c
    JOIN Products p ON c.CategoryID = p.CategoryID
    JOIN [Order Details] od ON p.ProductID = od.ProductID
    GROUP BY c.CategoryName
    ORDER BY TotalRevenue DESC
    """
    return pd.read_sql(query, engine)

def get_monthly_sales():
    """Retorna la tendencia de ventas por mes."""
    engine = get_engine()
    query = """
    SELECT 
        FORMAT(OrderDate, 'yyyy-MM') as Month,
        SUM(UnitPrice * Quantity) as MonthlyRevenue
    FROM Orders o
    JOIN [Order Details] od ON o.OrderID = od.OrderID
    GROUP BY FORMAT(OrderDate, 'yyyy-MM')
    ORDER BY Month
    """
    return pd.read_sql(query, engine)

def get_dim_products():
    """Extrae la dimensión de Productos."""
    query = "SELECT ProductID, ProductName, CategoryID, UnitPrice FROM Products"
    return pd.read_sql(query, get_engine())

def get_dim_customers():
    """Extrae la dimensión de Clientes."""
    query = "SELECT CustomerID, CompanyName, City, Country FROM Customers"
    return pd.read_sql(query, get_engine())

def get_fact_sales():
    """Extrae la tabla de hechos uniendo órdenes y detalles."""
    query = """
    SELECT 
        od.OrderID, o.OrderDate, od.ProductID, o.CustomerID,
        od.UnitPrice, od.Quantity, (od.UnitPrice * od.Quantity) as TotalAmount
    FROM [Order Details] od
    JOIN Orders o ON od.OrderID = o.OrderID
    """
    return pd.read_sql(query, get_engine())

def get_dim_calendar():
    """Genera una tabla de calendario basada en el rango de fechas de Northwind."""
    # Rango de fechas típico de la base de datos Northwind
    start_date = '1996-01-01'
    end_date = '1998-12-31'
    
    df = pd.DataFrame({'Date': pd.date_range(start_date, end_date)})
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['MonthName'] = df['Date'].dt.month_name()
    df['Quarter'] = df['Date'].dt.quarter
    df['DayOfWeek'] = df['Date'].dt.day_name()
    
    return df