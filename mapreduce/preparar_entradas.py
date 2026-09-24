"""
Genera los archivos de entrada para los programas MapReduce a partir del CSV de
GeoNames. Lee de stdin y escribe dos archivos locales que luego se suben a HDFS:

  nombres.txt   -> un nombre geográfico por línea           (wordmean, wordmedian)
  lat_dem.txt   -> "<franja_latitud> <elevacion_dem_m>"      (secondarysort)
                   franja_latitud = parte entera de la latitud (-90..90)
                   se omiten los DEM = -9999 (sin dato / océano)

Uso (en el nodo master de Dataproc):
  gcloud storage cat gs://BUCKET/raw/allCountries_headers.csv | python3 preparar_entradas.py
"""
import csv
import math
import sys

csv.field_size_limit(10**9)
lector = csv.DictReader(sys.stdin)
n = m = 0
with open("nombres.txt", "w") as f_nom, open("lat_dem.txt", "w") as f_sec:
    for fila in lector:
        n += 1
        f_nom.write(fila["name"].replace("\t", " ") + "\n")
        dem = fila["dem"]
        if dem and dem != "-9999":
            franja = math.floor(float(fila["latitude"]))
            f_sec.write(f"{franja} {int(dem)}\n")
            m += 1
print(f"nombres.txt: {n:,} líneas | lat_dem.txt: {m:,} pares", file=sys.stderr)
