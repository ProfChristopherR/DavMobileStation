# Guía de Ejecución — Sistema de Pronóstico IA (DAV Mobile)

Este módulo se ejecuta en el computador con Windows y realiza automáticamente el pronóstico meteorológico para las próximas **6 horas** utilizando el modelo **Amazon Chronos-Bolt** combinado con tu ecuación ponderada 40/30/30.

---

## 1. Requisitos Previos en Windows
- **Python 3.10 o 3.11** instalado (marcar la casilla *"Add Python to PATH"* durante la instalación).
- Conexión a Internet (la primera vez descarga los pesos del modelo ~200 MB desde Hugging Face).

---

## 2. Instalación del Entorno Virtual (Paso a Paso)

Abre la terminal de PowerShell en esta carpeta (`pronostico`) y ejecuta:

```powershell
# 1. Crear entorno virtual aislado
python -m venv venv

# 2. Activar el entorno virtual
.\venv\Scripts\Activate.ps1
# (Si Windows bloquea scripts, ejecuta una vez: Set-ExecutionPolicy Unrestricted -Scope Process)

# 3. Instalar PyTorch para CPU (optimizado para tu procesador sin GPU)
pip install torch --index-url https://download.pytorch.org/whl/cpu

# 4. Instalar las dependencias restantes
pip install -r requirements.txt
```

---

## 3. Configuración (.env)

1. Duplica el archivo `.env.example` y renómbralo a `.env`:
   ```powershell
   copy .env.example .env
   ```
2. Abre `.env` y coloca la URL de tu tabla `Pronostico_Estacion` de ArcGIS Online cuando la crees.

---

## 4. Ejecución Manual de Prueba

Con el entorno virtual activo:

```powershell
python pronostico_chronos.py
```

*Verás en la consola la descarga de datos desde ArcGIS, la inferencia de Chronos en CPU (< 1 segundo), la combinación híbrida y la creación del archivo de respaldo local `ultimo_pronostico.json`.*

---

## 5. Automatización en Windows (Cada 1 Hora)

Para que el computador corra el pronóstico solo cada 1 hora:

1. Abre el **Programador de Tareas** de Windows (`taskschd.msc`).
2. Haz clic en **Crear tarea básica...**
   - **Nombre:** `Pronostico DAV Mobile`
   - **Desencadenador:** Diariamente $\rightarrow$ repetir cada `1 hora`.
   - **Acción:** Iniciar un programa.
   - **Programa o script:** Busca el ejecutable de Python dentro de tu venv:
     `C:\Users\Dairo\OneDrive\Documents\Proyectos\LiceoSN\davmobile\pronostico\venv\Scripts\python.exe`
   - **Argumentos:** `pronostico_chronos.py`
   - **Iniciar en:** `C:\Users\Dairo\OneDrive\Documents\Proyectos\LiceoSN\davmobile\pronostico`
3. En las propiedades de la tarea, marca:
   - *"Ejecutar tanto si el usuario inició sesión como si no"*.
   - *"Despertar el equipo para ejecutar esta tarea"* (en Condiciones).
