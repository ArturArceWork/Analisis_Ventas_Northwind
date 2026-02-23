'''from src.database import run_query
from src.cleaning import clean_products
import os

def main():
    print("🚀 Iniciando Pipeline: Northwind ETL")
    
    # 1. Extraer
    query = "SELECT * FROM Products"
    raw_data = run_query(query)
    print(f"✅ Datos extraídos: {len(raw_data)} filas.")

    # 2. Transformar
    clean_data = clean_products(raw_data)
    print("✅ Transformación completada.")

    # 3. Cargar (Guardar en la carpeta data)
    output_path = os.path.join('data', 'products_cleaned.csv')
    clean_data.to_csv(output_path, index=False)
    print(f"💾 Archivo guardado en: {output_path}")

if __name__ == "__main__":
    main()'''

import os
from src.analytics import get_dim_products, get_dim_customers, get_fact_sales, get_dim_calendar

def main():
    print("🚀 Iniciando Pipeline: ANALISIS_VENTAS_NORTHWIND")
    
    if not os.path.exists('data'):
        os.makedirs('data')

    # Diccionario de exportación
    archivos = {
        'dim_productos.csv': get_dim_products,
        'dim_clientes.csv': get_dim_customers,
        'fact_ventas.csv': get_fact_sales,
        'dim_calendario.csv': get_dim_calendar
    }

    for nombre, funcion in archivos.items():
        print(f"📦 Generando {nombre}...")
        df = funcion()
        df.to_csv(os.path.join('data', nombre), index=False)
        print(f"✅ {nombre} listo.")

    print("\n🎉 Proceso terminado. Los 4 archivos están en la carpeta /data.")

if __name__ == "__main__":
    main()