#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
SISTEMA DE PRONÓSTICO METEOROLÓGICO IA — DAV MOBILE STATION
Liceo Bicentenario de Excelencia Polivalente San Nicolás
Modelo: Amazon Chronos-Bolt (Transformer Zero-Shot) + Ecuación Ponderada
=============================================================================
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
import pytz
import requests
import numpy as np
import pandas as pd
import torch
from dotenv import load_dotenv

# Configuración de Logging profesional
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pronostico.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("DAV_Pronostico")

# Cargar variables de entorno desde .env si existe
load_dotenv()

# --- CONFIGURACIÓN DEL SISTEMA ---
ESTACION_ID = os.getenv("ESTACION_ID", "EST_SAN_NICOLAS_01")
ARCGIS_QUERY_URL = os.getenv(
    "ARCGIS_QUERY_URL",
    "https://services4.arcgis.com/RZghiOOKLo2VeJGO/arcgis/rest/services/Estacion_Metereologica/FeatureServer/0/query"
)
ARCGIS_PRONOSTICO_URL = os.getenv(
    "ARCGIS_PRONOSTICO_URL",
    "https://services4.arcgis.com/RZghiOOKLo2VeJGO/arcgis/rest/services/Pronostico_Estacion/FeatureServer/0/addFeatures"
)
ARCGIS_USERNAME = os.getenv("ARCGIS_USERNAME", "")
ARCGIS_PASSWORD = os.getenv("ARCGIS_PASSWORD", "")
HORAS_PRONOSTICO = int(os.getenv("HORAS_PRONOSTICO", "6"))
DIAS_HISTORICO = int(os.getenv("DIAS_HISTORICO_CONSULTA", "7"))
MODELO_CHRONOS = os.getenv("MODELO_CHRONOS", "amazon/chronos-bolt-small")
ZONA_HORARIA = pytz.timezone("America/Santiago")

# =============================================================================
# 1. DESCARGA Y PREPROCESAMIENTO DE DATOS DE ARCGIS
# =============================================================================

def descargar_historico_arcgis(dias=DIAS_HISTORICO):
    """
    Descarga mediciones de ArcGIS Online de los últimos N días mediante paginación REST.
    """
    ahora_utc = datetime.now(pytz.UTC)
    desde_utc = ahora_utc - timedelta(days=dias)
    desde_ms = int(desde_utc.timestamp() * 1000)

    logger.info(f"Consultando datos desde {desde_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}...")
    
    registros = []
    offset = 0
    batch_size = 2000

    while True:
        params = {
            "where": f"fecha_lectura >= {desde_ms}",
            "outFields": "temperatura,humedad,presion,velocidad_viento,fecha_lectura",
            "returnGeometry": "false",
            "f": "json",
            "orderByFields": "fecha_lectura ASC",
            "resultRecordCount": batch_size,
            "resultOffset": offset
        }

        try:
            resp = requests.get(ARCGIS_QUERY_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Error al conectar con ArcGIS REST: {e}")
            break

        features = data.get("features", [])
        if not features:
            break

        for f in features:
            attrs = f.get("attributes", {})
            registros.append(attrs)

        logger.info(f"Descargados {len(features)} registros (acumulados: {len(registros)})...")

        # Comprobar si hay más registros
        if not data.get("exceededTransferLimit", False) and len(features) < batch_size:
            break

        offset += batch_size

    if not registros:
        logger.warning("No se encontraron registros en ArcGIS para el rango especificado.")
        return None

    df = pd.DataFrame(registros)
    logger.info(f"Total registros crudos obtenidos: {len(df)}")
    return df


def preparar_series_horarias(df_crudo):
    """
    Convierte timestamps, limpia outliers/NaNs y remuestrea a promedios por hora.
    """
    # 1. Conversión de fecha a zona horaria de Chile
    df = df_crudo.copy()
    df["fecha"] = pd.to_datetime(df["fecha_lectura"], unit="ms", utc=True).dt.tz_convert(ZONA_HORARIA)
    df = df.set_index("fecha").sort_index()

    # 2. Filtrado de campos
    columnas = ["temperatura", "humedad", "presion", "velocidad_viento"]
    for col in columnas:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3. Limpieza de valores físicamente imposibles
    df.loc[(df["temperatura"] < -20) | (df["temperatura"] > 55), "temperatura"] = np.nan
    df.loc[(df["humedad"] < 0) | (df["humedad"] > 100), "humedad"] = np.nan
    df.loc[(df["presion"] < 700) | (df["presion"] > 1100), "presion"] = np.nan
    df.loc[(df["velocidad_viento"] < 0) | (df["velocidad_viento"] > 150), "velocidad_viento"] = np.nan

    # 4. Agrupación y remuestreo a paso horario
    df_horario = df[columnas].resample("1h").mean()

    # 5. Interpolación lineal de huecos pequeños (hasta 2 horas consecutivas)
    df_horario = df_horario.interpolate(method="time", limit=2)

    # 6. Eliminar filas finales si tienen NaNs en todas las variables
    df_horario = df_horario.dropna(how="all")

    logger.info(f"Serie horaria lista: {len(df_horario)} horas procesadas (desde {df_horario.index[0]} hasta {df_horario.index[-1]}).")
    return df_horario


# =============================================================================
# 2. CARGA DEL MODELO CHRONOS (IA TRANSFORMER)
# =============================================================================

_pipeline_cache = None

def cargar_pipeline_chronos():
    """
    Carga el pipeline de Chronos-Bolt en CPU de forma optimizada.
    """
    global _pipeline_cache
    if _pipeline_cache is not None:
        return _pipeline_cache

    logger.info(f"Cargando modelo Chronos '{MODELO_CHRONOS}' en CPU...")
    try:
        from chronos import BaseChronosPipeline
        pipeline = BaseChronosPipeline.from_pretrained(
            MODELO_CHRONOS,
            device_map="cpu",
            torch_dtype=torch.float32
        )
    except ImportError:
        from chronos import ChronosPipeline
        pipeline = ChronosPipeline.from_pretrained(
            MODELO_CHRONOS,
            device_map="cpu",
            torch_dtype=torch.float32
        )

    logger.info("Modelo Chronos cargado exitosamente.")
    _pipeline_cache = pipeline
    return pipeline


def inferencia_chronos(pipeline, serie_valores, horizonte=HORAS_PRONOSTICO):
    """
    Ejecuta la inferencia de Chronos y extrae los cuantiles p10, p50 (mediana) y p90.
    """
    contexto = torch.tensor(serie_valores, dtype=torch.float32)

    try:
        # Chronos-Bolt retorna cuantiles directos
        forecast = pipeline.predict(contexto, prediction_length=horizonte)
    except TypeError:
        # Fallback a Chronos clásico con muestreo
        forecast = pipeline.predict(contexto, prediction_length=horizonte, num_samples=100)

    arr = forecast[0].detach().cpu().numpy()

    # Si retorna cuantiles (Chronos Bolt entrega 9 cuantiles: 0.1 a 0.9)
    if arr.ndim == 2 and arr.shape[0] == 9:
        p10 = arr[0]     # cuantil 0.1
        median = arr[4]  # cuantil 0.5 (mediana)
        p90 = arr[8]     # cuantil 0.9
    else:
        # Si retorna muestras de Monte Carlo
        p10, median, p90 = np.quantile(arr, [0.1, 0.5, 0.9], axis=0)

    return p10, median, p90


# =============================================================================
# 3. LÓGICA DE ECUACIÓN PONDERADA Y MODELO HÍBRIDO
# =============================================================================

def pronostico_ecuacion_horaria(df_horario, columna, timestamps_objetivo):
    """
    Calcula la ecuación ponderada adaptada a horas futuras:
    40% Promedio últimas 24h + 30% misma hora hace 7 días + 30% misma hora hace 3 días.
    Si faltan días históricos, realiza fallback automático a la media disponible.
    """
    serie = df_horario[columna].dropna()
    if len(serie) == 0:
        return [0.0] * len(timestamps_objetivo)

    # Promedio últimas 24 horas disponibles
    ultimas_24h = serie.iloc[-24:] if len(serie) >= 24 else serie
    promedio_24h = float(ultimas_24h.mean())

    predicciones = []

    for ts in timestamps_objetivo:
        # Búsqueda de misma hora hace 7 días y hace 3 días
        ts_hace_7d = ts - timedelta(days=7)
        ts_hace_3d = ts - timedelta(days=3)

        # Buscar valor más cercano dentro de ±1 hora
        def obtener_valor(ts_ref):
            if ts_ref in serie.index:
                return float(serie.loc[ts_ref])
            # Búsqueda por ventana de 1 hora
            cercanos = serie.loc[ts_ref - timedelta(hours=1): ts_ref + timedelta(hours=1)]
            if len(cercanos) > 0:
                return float(cercanos.iloc[0])
            return None

        v_7d = obtener_valor(ts_hace_7d)
        v_3d = obtener_valor(ts_hace_3d)

        # Ponderación dinámica según datos disponibles
        if v_7d is not None and v_3d is not None:
            val = (0.40 * promedio_24h) + (0.30 * v_7d) + (0.30 * v_3d)
        elif v_3d is not None:
            val = (0.60 * promedio_24h) + (0.40 * v_3d)
        elif v_7d is not None:
            val = (0.60 * promedio_24h) + (0.40 * v_7d)
        else:
            val = promedio_24h

        predicciones.append(float(val))

    return predicciones


def evaluar_alertas(temp, hum, viento, presion_actual, presion_futura):
    """
    Evalúa alertas meteorológicas considerando caída barométrica y umbrales clave.
    """
    alertas = []
    nivel = 0

    caida_p = (presion_actual - presion_futura) if (presion_actual and presion_futura) else 0.0

    # 1. Alerta de Temporal (Caída brusca de presión o alta combinación)
    if caida_p >= 2.5 and hum >= 80.0 and viento >= 25.0:
        alertas.append("ALERTA_TEMPORAL: Caida barometrica brusca con lluvia intensa y viento")
        nivel = max(nivel, 2)
    elif hum >= 88.0 and viento >= 28.0:
        alertas.append("AVISO_TEMPORAL: Alta humedad con vientos fuertes")
        nivel = max(nivel, 1)

    # 2. Alerta de Helada
    if temp <= 2.0:
        alertas.append(f"ALERTA_HELADA: Riesgo de congelamiento ({temp:.1f} C)")
        nivel = max(nivel, 2)
    elif temp <= 5.0:
        alertas.append(f"AVISO_FRIO: Bajas temperaturas matinales ({temp:.1f} C)")
        nivel = max(nivel, 1)

    # 3. Alerta de Viento
    if viento >= 40.0:
        alertas.append(f"ALERTA_VIENTO: Rafagas peligrosas pronosticadas ({viento:.1f} km/h)")
        nivel = max(nivel, 2)
    elif viento >= 25.0:
        alertas.append(f"AVISO_VIENTO: Viento moderado ({viento:.1f} km/h)")
        nivel = max(nivel, 1)

    if not alertas:
        alertas.append("CONDICIONES_ESTABLES")

    return "; ".join(alertas), nivel


# =============================================================================
# 4. ORQUESTADOR PRINCIPAL DEL PRONÓSTICO
# =============================================================================

def generar_pronostico_completo():
    """
    Descarga datos, ejecuta Chronos + Ecuación, genera alertas y publica a ArcGIS.
    """
    logger.info("=== INICIANDO CICLO DE PRONÓSTICO DAV MOBILE ===")

    # 1. Descarga de datos
    df_crudo = descargar_historico_arcgis(dias=DIAS_HISTORICO)
    if df_crudo is None or len(df_crudo) < 5:
        logger.error("No hay suficientes datos crudos para generar un pronóstico.")
        return None

    # 2. Preprocesamiento horario
    df_horario = preparar_series_horarias(df_crudo)
    if len(df_horario) < 6:
        logger.error("Se necesitan al menos 6 horas consecutivas de datos para pronosticar.")
        return None

    ultima_hora = df_horario.index[-1]
    timestamps_objetivo = [ultima_hora + timedelta(hours=i) for i in range(1, HORAS_PRONOSTICO + 1)]
    logger.info(f"Generando pronóstico para las próximas {HORAS_PRONOSTICO} horas: desde {timestamps_objetivo[0]} hasta {timestamps_objetivo[-1]}.")

    # 3. Cargar pipeline de Chronos
    pipeline = cargar_pipeline_chronos()

    variables_map = {
        "temperatura": "temperatura",
        "humedad": "humedad",
        "presion": "presion",
        "viento": "velocidad_viento"
    }

    pronosticos_por_variable = {}

    for var_nombre, col_df in variables_map.items():
        serie = df_horario[col_df].dropna().values

        # Si la variable no tiene datos reales (ej. sin sensor de presión), se omite
        if len(serie) < 6:
            logger.warning(f"Variable '{var_nombre}' sin datos suficientes ({len(serie)} h). Se publicará vacía.")
            continue
        
        # Inferencia Chronos
        p10, median, p90 = inferencia_chronos(pipeline, serie, horizonte=HORAS_PRONOSTICO)

        # Inferencia Ecuación Ponderada
        val_ecuacion = pronostico_ecuacion_horaria(df_horario, col_df, timestamps_objetivo)

        # Combinación Híbrida 50/50
        val_hibrido = (0.50 * np.array(val_ecuacion)) + (0.50 * median)

        # Aplicación de límites físicos meteorológicos
        if var_nombre == "humedad":
            val_hibrido = np.clip(val_hibrido, 5.0, 100.0)
            p10 = np.clip(p10, 5.0, 100.0)
            p90 = np.clip(p90, 5.0, 100.0)
        elif var_nombre == "viento":
            val_hibrido = np.maximum(val_hibrido, 0.0)
            p10 = np.maximum(p10, 0.0)
            p90 = np.maximum(p90, 0.0)

        pronosticos_por_variable[var_nombre] = {
            "hibrido": np.round(val_hibrido, 2),
            "p10": np.round(p10, 2),
            "p90": np.round(p90, 2),
            "ecuacion": np.round(val_ecuacion, 2),
            "chronos": np.round(median, 2)
        }

    # Presión actual para calcular delta
    presion_actual = float(df_horario["presion"].dropna().iloc[-1]) if not df_horario["presion"].dropna().empty else None

    if not all(v in pronosticos_por_variable for v in ("temperatura", "humedad", "viento")):
        logger.error("Faltan datos de temperatura, humedad o viento. No se puede pronosticar.")
        return None

    def val(var, clave, idx):
        """Devuelve el valor como float, o None si la variable no fue pronosticada."""
        if var not in pronosticos_por_variable:
            return None
        return float(pronosticos_por_variable[var][clave][idx])

    # 4. Armar los 6 registros horarios
    filas_arcgis = []
    fecha_ejecucion_ts = int(datetime.now(pytz.UTC).timestamp() * 1000)

    for h_idx in range(HORAS_PRONOSTICO):
        horizonte_num = h_idx + 1
        ts_obj = timestamps_objetivo[h_idx]
        ts_obj_ms = int(ts_obj.astimezone(pytz.UTC).timestamp() * 1000)

        t_val = val("temperatura", "hibrido", h_idx)
        h_val = val("humedad", "hibrido", h_idx)
        p_val = val("presion", "hibrido", h_idx)
        v_val = val("viento", "hibrido", h_idx)

        texto_alertas, nivel_alerta = evaluar_alertas(t_val, h_val, v_val, presion_actual, p_val)

        atributos = {
            "estacion_id": ESTACION_ID,
            "fecha_ejecucion": fecha_ejecucion_ts,
            "fecha_objetivo": ts_obj_ms,
            "horizonte_h": horizonte_num,
            "temperatura": t_val,
            "temperatura_p10": val("temperatura", "p10", h_idx),
            "temperatura_p90": val("temperatura", "p90", h_idx),
            "humedad": h_val,
            "humedad_p10": val("humedad", "p10", h_idx),
            "humedad_p90": val("humedad", "p90", h_idx),
            "presion": p_val,
            "presion_p10": val("presion", "p10", h_idx),
            "presion_p90": val("presion", "p90", h_idx),
            "viento": v_val,
            "viento_p10": val("viento", "p10", h_idx),
            "viento_p90": val("viento", "p90", h_idx),
            "alertas": texto_alertas,
            "nivel_alerta": nivel_alerta,
            "metodologia": "Chronos-Bolt-Small + Ecuación Ponderada 40/30/30"
        }

        filas_arcgis.append({"attributes": atributos})

    # Guardar copia local JSON para respaldo y depuración
    with open("ultimo_pronostico.json", "w", encoding="utf-8") as f:
        json.dump(filas_arcgis, f, indent=4, ensure_ascii=False)
    logger.info("Respaldo local guardado en 'ultimo_pronostico.json'.")

    # 5. Publicar en ArcGIS Online
    publicar_en_arcgis(filas_arcgis)

    return filas_arcgis


def publicar_en_arcgis(features):
    """
    Envía los registros pronosticados al servicio de ArcGIS Online via addFeatures.
    """
    logger.info(f"Publicando {len(features)} horas de pronóstico en ArcGIS Online...")

    payload = {
        "f": "json",
        "features": json.dumps(features)
    }

    try:
        resp = requests.post(ARCGIS_PRONOSTICO_URL, data=payload, timeout=15)
        resp.raise_for_status()
        resultado = resp.json()

        if "addResults" in resultado:
            exitos = sum(1 for r in resultado["addResults"] if r.get("success", False))
            logger.info(f"ArcGIS: {exitos}/{len(features)} registros insertados con éxito.")
        else:
            logger.warning(f"Respuesta inesperada de ArcGIS: {resultado}")
    except Exception as e:
        logger.error(f"Fallo al publicar en la tabla de pronósticos de ArcGIS: {e}")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    try:
        resultado = generar_pronostico_completo()
        if resultado:
            logger.info("Pronóstico generado y transmitido exitosamente.")
        else:
            logger.warning("El pronóstico no pudo completarse.")
    except Exception as e:
        logger.exception(f"Error crítico en la ejecución del pronóstico: {e}")
