import sys
import os
import site

print("Python version:", sys.version)
print("Executable:", sys.executable)
print("Python Path:", sys.path)
print("Site packages:", site.getsitepackages())

try:
    import pandas
    print("Pandas imported successfully. Version:", pandas.__version__)
except ImportError as e:
    print("Failed to import pandas:", e)

try:
    import meteostat
    print("Meteostat imported successfully.")
except ImportError as e:
    print("Failed to import meteostat:", e)

try:
    import arcgis
    print("ArcGIS imported successfully.")
except ImportError as e:
    print("Failed to import arcgis:", e)
