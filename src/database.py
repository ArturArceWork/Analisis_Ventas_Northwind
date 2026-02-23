import pandas as pd
from sqlalchemy import create_engine

def get_engine():
    """Crea y retorna el motor de conexión a SQL Server."""
    SERVER = 'localhost'
    DATABASE = 'instnwnd'
    DRIVER = 'ODBC Driver 18 for SQL Server'
    
    connection_url = (
        f"mssql+pyodbc://@{SERVER}/{DATABASE}?"
        f"driver={DRIVER}&"
        f"trusted_connection=yes&"
        f"Encrypt=yes&"
        f"TrustServerCertificate=yes"
    )
    return create_engine(connection_url)

def run_query(query):
    """Ejecuta una consulta y retorna un DataFrame."""
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(query, conn)