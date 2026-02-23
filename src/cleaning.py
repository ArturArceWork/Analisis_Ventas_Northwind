import pandas as pd

def clean_products(df):
    """Limpieza básica de la tabla de productos."""
    # Convertir nombres de columnas a minúsculas
    df.columns = [col.lower() for col in df.columns]
    
    # Manejar nulos en precios (si los hubiera)
    df['unitprice'] = df['unitprice'].fillna(0)
    
    # Crear una columna de inventario total valorizado
    df['inventory_value'] = df['unitprice'] * df['unitsinstock']
    
    return df