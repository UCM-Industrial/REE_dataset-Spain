import pandas as pd
from pathlib import Path

# ======================================================
# OPCIONES DEL USUARIO
# ======================================================
desktop_path = Path.home() / "Desktop"
input_file = desktop_path / "energy_generation_MWh.xlsx"   # Cambiar a MW si corresponde

# Modo de agrupación: elegir uno entre "hour", "day", "week", "month", "year"
group_mode = "week"

# ======================================================
# LECTURA DE DATOS
# ======================================================
df = pd.read_excel(input_file)

# Reconstruir índice datetime combinando fecha y hora
df['datetime'] = pd.to_datetime(df['date'].astype(str) + " " + df['time'].astype(str))
df = df.set_index('datetime')

# Eliminar columnas auxiliares
df = df.drop(columns=['date', 'time'])

# ======================================================
# AGRUPACIONES SEGÚN MODO
# ======================================================
if group_mode == "hour":
    # Promedio por hora del día (ejemplo: todas las 10:00 de todos los días)
    result = df.groupby(df.index.hour).mean()
    out_file = desktop_path / "energy_generation_grouped_hourly.xlsx"

elif group_mode == "day":
    # Total diario
    result = df.resample('D').sum()
    out_file = desktop_path / "energy_generation_grouped_daily.xlsx"

elif group_mode == "week":
    # Total semanal
    result = df.resample('W').sum()
    out_file = desktop_path / "energy_generation_grouped_weekly.xlsx"

elif group_mode == "month":
    # Total mensual
    result = df.resample('M').sum()
    # Cambiar índice a formato Año-Mes
    result.index = result.index.to_period('M').astype(str)
    out_file = desktop_path / "energy_generation_grouped_monthly.xlsx"

elif group_mode == "year":
    # Total anual
    result = df.resample('Y').sum()
    result.index = result.index.year
    out_file = desktop_path / "energy_generation_grouped_yearly.xlsx"

else:
    raise ValueError("El parámetro group_mode debe ser 'hour', 'day', 'week', 'month' o 'year'.")

# ======================================================
# EXPORTACIÓN
# ======================================================
result.to_excel(out_file, engine="openpyxl")
print(f"Archivo agrupado por {group_mode} guardado en:", out_file)
