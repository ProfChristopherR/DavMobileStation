import pandas as pd
import requests
from datetime import datetime, timedelta
from arcgis.gis import GIS
import os
import pytz
import glob
import urllib3

# Desactivar advertencias de SSL no verificado
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
# Coordenadas de San Nicolás, Ñuble
LATITUD = -36.50
LONGITUD = -72.21

ARCGIS_USERNAME = os.getenv('ARCGIS_USERNAME')
ARCGIS_PASSWORD = os.getenv('ARCGIS_PASSWORD')
FEATURE_LAYER_ITEM_ID = '80531cc9dc43460380c4d120d02fd293'

# Configuración de carpetas y columnas (según tu CSV)
CARPETA_HISTORICO = 'historico' 
COL_FECHA_CSV = 'momento'       
COL_TEMP_CSV = 'ts'             

# Configuración Agromet (Legacy) y ArcGIS
CAMPOS_ARCGIS = {
    'tavg': 'temperatura_pronostico',
    'tmax': 'tempmax',
    'tmin': 'tempmin'
}

# ==========================================
# 2. PROCESAMIENTO DE HISTÓRICO LOCAL
# ==========================================

def cargar_historico_local():
    """Lee todos los CSV de la carpeta historico y crea promedios, max y min diarios."""
    print(f"DEBUG: Iniciando carga de archivos históricos desde '{CARPETA_HISTORICO}'...")
    archivos = glob.glob(os.path.join(CARPETA_HISTORICO, "*.csv"))
    if not archivos:
        print("ERROR: No se encontraron archivos CSV en la carpeta 'historico'.")
        return None
    
    print(f"DEBUG: Se encontraron {len(archivos)} archivos CSV.")
    lista_df = []
    for f in archivos:
        try:
            # Usamos sep=';' porque tu archivo de ejemplo usa punto y coma
            temp_df = pd.read_csv(f, sep=';', parse_dates=[COL_FECHA_CSV])
            lista_df.append(temp_df)
            print(f"DEBUG: Archivo '{os.path.basename(f)}' cargado exitosamente.")
        except Exception as e:
            print(f"ERROR: No se pudo leer el archivo '{f}': {e}")
    
    if not lista_df:
        print("ERROR: Ningún archivo pudo ser cargado.")
        return None

    df_full = pd.concat(lista_df)
    print(f"DEBUG: Total de registros históricos combinados: {len(df_full)}")
    
    # Agrupar por día para tener estadísticas reales
    df_diario = df_full.groupby(df_full[COL_FECHA_CSV].dt.floor('d'))[COL_TEMP_CSV].agg(['mean', 'max', 'min'])
    df_diario.columns = ['tavg', 'tmax', 'tmin']
    
    print("DEBUG: Resumen del histórico local cargado:")
    print(df_diario.head())
    return df_diario

# ==========================================
# 3. OBTENCIÓN DE DATOS RECIENTES (WEB)
# ==========================================

def obtener_datos_actuales():
    """Obtiene datos recientes de Open-Meteo como fuente confiable por coordenadas."""
    print(f"DEBUG: Conectando a Open-Meteo para coordenadas ({LATITUD}, {LONGITUD})...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LATITUD,
        "longitude": LONGITUD,
        "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"],
        "timezone": "America/Santiago", # Forzamos la zona horaria en la API también
        "past_days": 0,                 # No queremos días pasados en el resultado actual
        "forecast_days": 7              # Pedimos 7 días para tener margen
    }
    
    try:
        # Añadimos verify=False para evitar errores de SSL en algunos entornos locales
        r = requests.get(url, params=params, timeout=15, verify=False)
        r.raise_for_status()
        data = r.json()
        print("DEBUG: Datos recibidos de Open-Meteo.")

        df = pd.DataFrame(data['daily'])
        df['fecha'] = pd.to_datetime(df['time'])
        df = df.set_index('fecha')
        
        # Mapear a nombres de columnas internos
        res = pd.DataFrame(index=df.index)
        res['tavg'] = df['temperature_2m_mean']
        res['tmax'] = df['temperature_2m_max']
        res['tmin'] = df['temperature_2m_min']
        
        print("DEBUG: Datos recientes procesados (Open-Meteo):")
        print(res.tail(3))
        return res
    except Exception as e:
        print(f"ERROR: Falló la conexión con Open-Meteo: {e}")
        return None

# ==========================================
# 4. CÁLCULO CON TU FÓRMULA
# ==========================================

def calcular_pronostico_variable(serie_reciente, serie_historica):
    # Definimos la zona horaria de Chile
    tz_chile = pytz.timezone('America/Santiago')
    # Obtenemos la fecha actual en Chile y eliminamos la hora
    hoy_chile = datetime.now(tz_chile).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # IMPORTANTE: Convertimos hoy_chile a un datetime "naive" (sin zona horaria) 
    # para que sea compatible con los índices de tus DataFrames de pandas
    hoy = hoy_chile.replace(tzinfo=None)
    # Tomamos los últimos 7 días como base
    valores_dinamicos = serie_reciente.tail(7).tolist()
    resultados = []

    for i in range(1, 5):
        fecha_p = hoy + timedelta(days=i)
        
        # --- PARTE 40%: Promedio histórico (Mismo día/mes años anteriores) ---
        try:
            mask = (serie_historica.index.month == fecha_p.month) & (serie_historica.index.day == fecha_p.day)
            promedio_hist = serie_historica[mask].mean()
            if pd.isna(promedio_hist): 
                promedio_hist = valores_dinamicos[-1]
        except:
            promedio_hist = valores_dinamicos[-1]

        # --- PARTE 30% + 30%: Tendencia ---
        promedio_7d = sum(valores_dinamicos[-7:]) / 7
        promedio_3d = sum(valores_dinamicos[-3:]) / 3
        
        # Fórmula Final
        final = (0.40 * promedio_hist) + (0.30 * promedio_7d) + (0.30 * promedio_3d)
        
        resultados.append({
            'fecha_dt': fecha_p,
            'valor': round(final, 2)
        })
        valores_dinamicos.append(final) # Recursividad
        
    return resultados

def generar_pronosticos_completos(df_reciente, df_historico):
    print("DEBUG: Calculando pronósticos para TAvg, TMax y TMin...")
    pronosticos_por_dia = {}
    
    for var in ['tavg', 'tmax', 'tmin']:
        resultados_var = calcular_pronostico_variable(df_reciente[var], df_historico[var])
        for res in resultados_var:
            fecha_str = res['fecha_dt'].strftime('%Y-%m-%d')
            if fecha_str not in pronosticos_por_dia:
                pronosticos_por_dia[fecha_str] = {
                    'fecha_agol': int(res['fecha_dt'].timestamp() * 1000),
                    'fecha_texto': res['fecha_dt'].date()
                }
            pronosticos_por_dia[fecha_str][var] = res['valor']
    
    return list(pronosticos_por_dia.values())

# ==========================================
# 5. ACTUALIZACIÓN ARCGIS
# ==========================================

def actualizar_arcgis(datos):
    print(f"DEBUG: Conectando a ArcGIS Online como '{ARCGIS_USERNAME}'...")
    try:
        gis = GIS("https://www.arcgis.com", ARCGIS_USERNAME, ARCGIS_PASSWORD)
        item = gis.content.get(FEATURE_LAYER_ITEM_ID)
        if not item:
            print(f"ERROR: No se encontró el item con ID '{FEATURE_LAYER_ITEM_ID}' en ArcGIS.")
            return
        
        capa = item.layers[0]
        print(f"DEBUG: Conectado exitosamente a la capa: '{item.title}'")
        
        features = []
        for d in datos:
            atributos = { "fecha": d['fecha_agol'] }
            for var, campo_agol in CAMPOS_ARCGIS.items():
                # Aseguramos que sean floats nativos de Python para evitar errores de tipo en ArcGIS
                atributos[campo_agol] = float(d[var])
            
            features.append({"attributes": atributos})
        
        print(f"DEBUG: Preparados {len(features)} registros para subir.")
        print("DEBUG: Payload ejemplo (primer registro):", features[0])

        print("DEBUG: Eliminando registros anteriores...")
        capa.delete_features(where="1=1")
        
        print("DEBUG: Subiendo nuevos pronósticos...")
        resultado = capa.edit_features(adds=features)
        
        print(f"DEBUG: Resultado de la subida: {resultado}")
        print("SUCCESS: Sincronización exitosa con ArcGIS Online.")
    except Exception as e:
        print(f"ERROR: Falló la actualización de ArcGIS: {e}")

# ==========================================
# EJECUCIÓN PRINCIPAL
# ==========================================

if __name__ == "__main__":
    print(f"\n--- INICIO DE PROCESO: {datetime.now()} ---")
    
    hist = cargar_historico_local()
    if hist is None:
        print("FATAL: No se pudo cargar el historial local. Abortando.")
    else:
        reciente = obtener_datos_actuales()
        if reciente is None:
            print("FATAL: No se pudieron obtener datos recientes de Agromet. Abortando.")
        else:
            pronosticos = generar_pronosticos_completos(reciente, hist)
            print("\nDEBUG: Pronósticos generados:")
            for p in pronosticos:
                print(f"  {p['fecha_texto']}: TAvg={p['tavg']}°C, TMax={p['tmax']}°C, TMin={p['tmin']}°C")
            
            actualizar_arcgis(pronosticos)
    
    print(f"--- FIN DE PROCESO: {datetime.now()} ---\n")
