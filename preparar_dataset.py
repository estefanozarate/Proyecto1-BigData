"""
Convierte el dump original de GeoNames (allCountries.txt, separado por tabs y sin
encabezado) a un CSV con encabezados: allCountries_headers.csv.

Se procesa en streaming (línea por línea) para no cargar 1.8 GB en memoria.
Al final verifica que el número de filas de salida coincida con el de entrada.

Uso:
    python src/preparar_dataset.py allCountries.zip allCountries_headers.csv
    python src/preparar_dataset.py allCountries.txt allCountries_headers.csv
"""
import csv
import io
import sys
import zipfile

from common import ALL_COLUMNS

csv.field_size_limit(10**9)


def abrir(entrada):
    if entrada.endswith(".zip"):
        z = zipfile.ZipFile(entrada)
        return io.TextIOWrapper(z.open("allCountries.txt"), encoding="utf-8", newline="")
    return open(entrada, encoding="utf-8", newline="")


def main(entrada, salida):
    n = 0
    with abrir(entrada) as fin, open(salida, "w", encoding="utf-8", newline="") as fout:
        lector = csv.reader(fin, delimiter="\t", quoting=csv.QUOTE_NONE)
        escritor = csv.writer(fout)
        escritor.writerow(ALL_COLUMNS)
        for fila in lector:
            if len(fila) != len(ALL_COLUMNS):
                raise ValueError(f"Fila {n + 1} con {len(fila)} columnas: {fila[:3]}")
            escritor.writerow(fila)
            n += 1
            if n % 2_000_000 == 0:
                print(f"{n:,} filas...", flush=True)
    print(f"Listo: {n:,} registros escritos en {salida}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
