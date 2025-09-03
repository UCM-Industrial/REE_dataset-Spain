import requests
import json
import pandas as pd
import time
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm

# Carpeta de salida: escritorio del usuario
desktop_path = Path.home() / "Desktop"

# ======================================================
# OPCIONES DEL USUARIO
# ======================================================
# Rango de fechas para la descarga (ajustar según necesidad)
start_date = datetime(2024, 1, 1)
end_date   = datetime(2024, 12, 31)

# Opción de conversión: 
# - Si True, los datos se guardan en MWh (energía en cada intervalo de 5 minutos)
# - Si False, se guardan en MW (potencia instantánea)
convert_to_mwh = True

# ======================================================
# CRONÓMETRO PARA MEDIR TIEMPO DE EJECUCIÓN
# ======================================================
start_time = time.time()

# ======================================================
# CREACIÓN DEL LISTADO DE FECHAS
# ======================================================
# Genera todas las fechas entre start_date y end_date en formato YYYY-MM-DD
dates = [(start_date + timedelta(days=d)).strftime('%Y-%m-%d')
         for d in range((end_date - start_date).days + 1)]

# URL base del servicio de REE
url_base = "https://demanda.ree.es/WSvisionaMovilesPeninsulaRest/resources/demandaGeneracionPeninsula?callback=angular.callbacks._3&curva=DEMANDAQH&fecha="

# ======================================================
# FUNCIÓN PARA DESCARGAR LOS DATOS DE UN DÍA
# ======================================================
def fetch_date(date):
    """
    Descarga los datos de generación y demanda para una fecha concreta.
    Devuelve una lista de registros horarios (5 minutos) o una lista vacía si falla.
    """
    url = url_base + date
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            # La respuesta es JSONP: se elimina el envoltorio "angular.callbacks"
            jsonp = r.text
            data = json.loads(jsonp.split('angular.callbacks._3(')[1].rsplit(')', 1)[0])
            return data['valoresHorariosGeneracion'][37:325]  # Filtrado de los valores horarios
    except Exception as e:
        print(f"Error en {date}: {e}")
    return []

# ======================================================
# DESCARGA EN PARALELO
# ======================================================
# Usamos un ThreadPoolExecutor para acelerar las descargas
all_data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_date, d) for d in dates]
    for f in tqdm(as_completed(futures), total=len(futures)):
        all_data.extend(f.result())

# ======================================================
# PROCESAMIENTO DE LOS DATOS
# ======================================================
# Conversión a DataFrame
df = pd.DataFrame(all_data)

# Limpieza de timestamps especiales en días de cambio horario (2A y 2B → 02:00)
df['ts'] = df['ts'].astype(str).str.replace('2A:', '02:').str.replace('2B:', '02:')

# Conversión segura a tipo datetime
df['ts'] = pd.to_datetime(df['ts'], errors='coerce')

# Eliminación de duplicados: si existen dos filas con la misma hora (2A y 2B),
# se calculan promedios de las columnas numéricas
df = df.groupby('ts').mean().reset_index()

# Crear columnas separadas de fecha y hora para facilitar análisis en Excel
df['date'] = df['ts'].dt.date
df['time'] = df['ts'].dt.time

# Reordenar columnas: primero date, luego time, luego los datos
cols = ['date', 'time'] + [c for c in df.columns if c not in ['date', 'time']]
df = df[cols]

# Conversión de MW a MWh si corresponde
# Cada registro representa 5 minutos → 5/60 horas = 1/12 de hora
if convert_to_mwh:
    df.iloc[:, 3:] = df.iloc[:, 3:].apply(lambda col: col * (5/60))
    out_file = desktop_path / "energy_generation_MWh.xlsx"
else:
    out_file = desktop_path / "energy_generation_MW.xlsx"

# ======================================================
# EXPORTACIÓN A EXCEL
# ======================================================
df.to_excel(out_file, index=False, engine="openpyxl")

# ======================================================
# TIEMPO DE EJECUCIÓN
# ======================================================
elapsed = time.time() - start_time
print("Archivo generado:", out_file)
print(f"Tiempo total de ejecución: {elapsed:.2f} segundos")
