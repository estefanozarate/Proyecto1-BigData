"""
Procesamiento del dataset GeoNames con Polars (motor multihilo en Rust, API lazy).

Uso:
    python src/polars_queries.py --input gs://utec-bigdata-geonames-2026/raw/allCountries_headers.csv
    python src/polars_queries.py --input data/allCountries_headers.csv --write-processed out/polars
"""
import argparse
import os

import polars as pl

from common import (DEFAULT_INPUT, DUP_KEYS, FCLASS_DESC, POP_LABELS, SOUTH_AMERICA,
                    USE_COLUMNS, Timer, header, save_result)

FW = "polars"

SCHEMA = {
    "geonameid": pl.Int64, "name": pl.Utf8,
    "latitude": pl.Float64, "longitude": pl.Float64, "fclass": pl.Utf8,
    "fcode": pl.Utf8, "country": pl.Utf8, "admin1": pl.Utf8,
    "population": pl.Int64, "elevation": pl.Float64, "dem": pl.Int64,
    "timezone": pl.Utf8, "moddate": pl.Utf8,
}


def load(path):
    # Lectura lazy con proyección de columnas y tipos explícitos
    return (
        pl.scan_csv(path, schema_overrides=SCHEMA, infer_schema_length=0)
        .select(USE_COLUMNS)
    )


def run(path, out_dir, write_processed=None):
    t = Timer(FW)
    out = os.path.join(out_dir, FW)

    with t.measure("carga"):
        # Tipos categóricos para columnas de baja cardinalidad (menos memoria)
        df = (load(path)
              .with_columns(pl.col("fclass", "fcode", "country", "timezone")
                            .cast(pl.Categorical))
              .collect())
    print(f"Registros leidos: {df.height:,} | columnas: {df.width}")

    # ------------------------------------------------------------------ Q1
    header("Q1")
    with t.measure("Q1"):
        nulos = df.null_count().transpose(include_header=True,
                                         column_names=["nulos"]).rename({"column": "columna"})
        df = df.with_columns(
            pl.col("name").str.strip_chars(),
            pl.col("population").fill_null(0),
            pl.when(pl.col("dem") == -9999).then(None).otherwise(pl.col("dem")).alias("dem"),
            pl.col("moddate").str.to_date("%Y-%m-%d", strict=False),
        )
    save_result(nulos.to_pandas(), out, "Q1", show=20)

    # ------------------------------------------------------------------ Q2
    header("Q2")
    with t.measure("Q2"):
        n0 = df.height
        dup_id = n0 - df.select(pl.col("geonameid").n_unique()).item()
        df = df.unique(subset=["geonameid"], keep="first")
        n1 = df.height
        # Duplicados lógicos (mismo nombre+país+coordenadas): se detectan con un
        # hash de 64 bits de la clave compuesta (más liviano que agrupar por 4
        # columnas de texto) y se conserva el menor geonameid de cada grupo
        clave = pl.struct(DUP_KEYS).hash()
        en_dup = df.filter(clave.is_duplicated()).select(*DUP_KEYS, "geonameid")
        eliminar = en_dup.filter(pl.col("geonameid") != pl.col("geonameid").min().over(DUP_KEYS))
        df = df.filter(~pl.col("geonameid").is_in(eliminar.get_column("geonameid").implode()))
        dup_comp = n1 - df.height
        q2 = pl.DataFrame({
            "criterio": ["geonameid", "name+country+lat+lon", "registros_finales"],
            "valor": [dup_id, dup_comp, df.height],
        })
    save_result(q2.to_pandas(), out, "Q2")

    # ------------------------------------------------------------------ Q3
    header("Q3")
    with t.measure("Q3"):
        reglas = {
            "latitud_fuera_rango": ~pl.col("latitude").is_between(-90, 90),
            "longitud_fuera_rango": ~pl.col("longitude").is_between(-180, 180),
            "poblacion_negativa": pl.col("population") < 0,
            "pais_nulo": pl.col("country").is_null(),
            "nombre_nulo": pl.col("name").is_null(),
        }
        conteos = df.select([e.fill_null(False).sum().alias(k) for k, e in reglas.items()])
        invalido = pl.any_horizontal([e.fill_null(False) for e in reglas.values()])
        antes = df.height
        df = df.filter(~invalido)                         # DELETE
        q3 = conteos.transpose(include_header=True, column_names=["registros"]) \
                    .rename({"column": "regla"})
        q3 = pl.concat([q3, pl.DataFrame({"regla": ["TOTAL_ELIMINADOS"],
                                          "registros": [antes - df.height]},
                                         schema=q3.schema)])
    save_result(q3.to_pandas(), out, "Q3")

    # ------------------------------------------------------------------ Q4
    header("Q4")
    with t.measure("Q4"):
        df = df.with_columns(                              # CREATE / UPDATE
            pl.col("moddate").dt.year().alias("anio_mod"),
            pl.when(pl.col("latitude") >= 0).then(pl.lit("Norte"))
              .otherwise(pl.lit("Sur")).alias("hemisferio"),
            pl.coalesce(pl.col("elevation"), pl.col("dem").cast(pl.Float64))
              .alias("elevacion_final"),
            # Población solo de centros poblados (fclass P): evita contar dos veces
            # a la misma gente en divisiones administrativas (A) y en ciudades (P)
            pl.when(pl.col("fclass") == "P").then(pl.col("population"))
              .otherwise(0).alias("poblacion_asentamiento"),
            pl.col("fclass").cast(pl.Utf8).replace_strict(FCLASS_DESC, default="Desconocido")
              .alias("fclass_desc"),
            pl.when(pl.col("population") <= 0).then(pl.lit(POP_LABELS[0]))
              .when(pl.col("population") < 1_000).then(pl.lit(POP_LABELS[1]))
              .when(pl.col("population") < 100_000).then(pl.lit(POP_LABELS[2]))
              .when(pl.col("population") < 1_000_000).then(pl.lit(POP_LABELS[3]))
              .otherwise(pl.lit(POP_LABELS[4])).alias("categoria_poblacion"),
        )
        q4 = (df.group_by("categoria_poblacion").len("registros")
                .sort("registros", descending=True))
    save_result(q4.to_pandas(), out, "Q4")

    # ------------------------------------------------------------------ Q5
    header("Q5")
    with t.measure("Q5"):
        q5 = (df.filter((pl.col("country") == "PE") & (pl.col("fclass") == "P")
                        & (pl.col("population") > 0))
                .sort("population", descending=True)
                .select("name", "admin1", "population", "elevacion_final",
                        "latitude", "longitude"))
    print(f"Centros poblados con poblacion en Peru: {q5.height:,}")
    save_result(q5.head(20).to_pandas(), out, "Q5")

    # ------------------------------------------------------------------ Q6
    header("Q6")
    with t.measure("Q6"):
        q6 = (df.group_by("country")
                .agg(pl.len().alias("registros"),
                     pl.col("poblacion_asentamiento").sum().alias("poblacion_total"),
                     pl.col("elevacion_final").mean().round(2).alias("elevacion_promedio"))
                .sort("poblacion_total", descending=True))
    save_result(q6.to_pandas(), out, "Q6")

    # ------------------------------------------------------------------ Q7
    header("Q7")
    with t.measure("Q7"):
        total = df.height
        q7 = (df.group_by("fclass", "fclass_desc").len("registros")
                .with_columns((pl.col("registros") / total * 100).round(2).alias("porcentaje"))
                .sort("registros", descending=True))
    save_result(q7.to_pandas(), out, "Q7")

    # ------------------------------------------------------------------ Q8
    header("Q8")
    with t.measure("Q8"):
        q8 = (df.filter((pl.col("fclass") == "P") & (pl.col("population") > 1_000_000))
                .group_by("country")
                .agg(pl.len().alias("ciudades_mas_1M"),
                     pl.col("population").sum().alias("poblacion_en_esas_ciudades"))
                .sort(["ciudades_mas_1M", "poblacion_en_esas_ciudades"],
                      descending=[True, True]))
    save_result(q8.to_pandas(), out, "Q8")

    # ------------------------------------------------------------------ Q9
    header("Q9")
    with t.measure("Q9"):
        q9 = (df.filter(pl.col("country").cast(pl.Utf8).is_in(SOUTH_AMERICA) & (pl.col("fclass") == "P"))
                .with_columns(pl.col("population").rank("ordinal", descending=True)
                              .over("country").alias("ranking"))
                .filter(pl.col("ranking") <= 3)
                .select("country", "ranking", "name", "population")
                .sort(["country", "ranking"]))
    save_result(q9.to_pandas(), out, "Q9", show=36)

    # ------------------------------------------------------------------ Q10
    header("Q10")
    with t.measure("Q10"):
        q10 = (df.group_by("anio_mod").len("registros")
                 .drop_nulls("anio_mod")
                 .sort("anio_mod")
                 .with_columns((pl.col("registros").cum_sum() / pl.col("registros").sum() * 100)
                               .round(2).alias("pct_acumulado")))
    save_result(q10.to_pandas(), out, "Q10", show=30)

    if write_processed:
        with t.measure("escritura_parquet"):
            df.write_parquet(os.path.join(write_processed, "geonames_clean.parquet"))

    t.times["total_consultas"] = round(sum(v for k, v in t.times.items()
                                           if k.startswith("Q")), 3)
    print(f"\nTiempos guardados en {t.save(out_dir)}")
    return t.times


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=DEFAULT_INPUT)
    ap.add_argument("--out", default="results")
    ap.add_argument("--write-processed", default=None)
    a = ap.parse_args()
    run(a.input, a.out, a.write_processed)
