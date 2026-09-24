"""
Uso:
    python src/dask_queries.py --input gs://utec-bigdata-geonames-2026/raw/allCountries_headers.csv
    python src/dask_queries.py --input data/allCountries_headers.csv --write-processed out/dask
"""
import argparse
import os

import dask
import dask.dataframe as dd
import pandas as pd

from common import (DEFAULT_INPUT, DUP_KEYS, FCLASS_DESC, PANDAS_DTYPES, POP_BINS,
                    POP_LABELS, SOUTH_AMERICA, USE_COLUMNS, Timer, header,
                    save_result)

FW = "dask"


def load(path):
    return dd.read_csv(path, usecols=USE_COLUMNS, dtype=PANDAS_DTYPES,
                       blocksize="64MB", keep_default_na=False,
                       na_values=[""])


def categorizar(pop):
    return pd.cut(pop, bins=POP_BINS, labels=POP_LABELS, right=False).astype(str)


def run(path, out_dir, write_processed=None):
    t = Timer(FW)
    out = os.path.join(out_dir, FW)

    with t.measure("carga"):
        df = load(path)
        n_total = len(df)          # fuerza la lectura completa
    print(f"Registros leidos: {n_total:,} | particiones: {df.npartitions}")

    # ------------------------------------------------------------------ Q1
    header("Q1")
    with t.measure("Q1"):
        nulos = df.isna().sum().compute()
        df = df.assign(
            name=df["name"].str.strip(),
            population=df["population"].fillna(0).astype("int64"),
            dem=df["dem"].mask(df["dem"] == -9999),
            moddate=dd.to_datetime(df["moddate"], format="%Y-%m-%d", errors="coerce"),
        )
    save_result(nulos.rename_axis("columna").reset_index(name="nulos"), out, "Q1", show=20)

    # ------------------------------------------------------------------ Q2
    header("Q2")
    with t.measure("Q2"):
        dup_id = n_total - df["geonameid"].nunique().compute()
        df = df.drop_duplicates(subset=["geonameid"], split_out=df.npartitions)
        n1 = len(df)
        # Duplicados lógicos (mismo nombre+país+coordenadas): hash de 64 bits de
        # la clave compuesta por partición -> groupby distribuido (split_out)
        # -> solo se traen al driver los grupos repetidos (pocos miles)
        k = df[DUP_KEYS].map_partitions(
            lambda p: pd.util.hash_pandas_object(p, index=False), meta=("k", "uint64"))
        tmp = df[["geonameid"]].assign(k=k)
        g = tmp.groupby("k")["geonameid"].agg(["count", "min"], split_out=8)
        grupos = g[g["count"] > 1].compute()
        en_dup = tmp[tmp["k"].isin(grupos.index.values)].compute().join(grupos["min"], on="k")
        eliminar = en_dup.loc[en_dup["geonameid"] != en_dup["min"], "geonameid"].values
        df = df[~df["geonameid"].isin(eliminar)]
        df = df.persist()          # se cachea el DataFrame deduplicado
        n2 = len(df)
        q2 = pd.DataFrame({"criterio": ["geonameid", "name+country+lat+lon", "registros_finales"],
                           "valor": [dup_id, n1 - n2, n2]})
    save_result(q2, out, "Q2")

    # ------------------------------------------------------------------ Q3
    header("Q3")
    with t.measure("Q3"):
        reglas = {
            "latitud_fuera_rango": ~df["latitude"].between(-90, 90),
            "longitud_fuera_rango": ~df["longitude"].between(-180, 180),
            "poblacion_negativa": df["population"] < 0,
            "pais_nulo": df["country"].isna(),
            "nombre_nulo": df["name"].isna(),
        }
        conteos = dask.compute(*[m.sum() for m in reglas.values()])
        invalido = reglas["latitud_fuera_rango"]
        for m in list(reglas.values())[1:]:
            invalido = invalido | m
        df = df[~invalido]                                   # DELETE
        eliminados = n2 - len(df)
        q3 = pd.DataFrame({"regla": list(reglas) + ["TOTAL_ELIMINADOS"],
                           "registros": list(conteos) + [eliminados]})
    save_result(q3, out, "Q3")

    # ------------------------------------------------------------------ Q4
    header("Q4")
    with t.measure("Q4"):
        es_p = df["fclass"] == "P"
        df = df.assign(                                      # CREATE / UPDATE
            anio_mod=df["moddate"].dt.year,
            hemisferio=df["latitude"].map_partitions(
                lambda s: s.ge(0).map({True: "Norte", False: "Sur"}), meta=("hemisferio", "object")),
            elevacion_final=df["elevation"].fillna(df["dem"]),
            poblacion_asentamiento=df["population"].where(es_p, 0),
            fclass_desc=df["fclass"].astype("object").map(FCLASS_DESC, meta=("fclass_desc", "object"))
                         .fillna("Desconocido"),
            categoria_poblacion=df["population"].map_partitions(
                categorizar, meta=("categoria_poblacion", "object")),
        )
        df = df.persist()
        q4 = (df.groupby("categoria_poblacion").size().compute()
                .sort_values(ascending=False).reset_index(name="registros"))
    save_result(q4, out, "Q4")

    # ------------------------------------------------------------------ Q5
    header("Q5")
    with t.measure("Q5"):
        q5 = (df[(df["country"] == "PE") & (df["fclass"] == "P") & (df["population"] > 0)]
              [["name", "admin1", "population", "elevacion_final", "latitude", "longitude"]]
              .compute()
              .sort_values("population", ascending=False))
    print(f"Centros poblados con poblacion en Peru: {len(q5):,}")
    save_result(q5.head(20), out, "Q5")

    # ------------------------------------------------------------------ Q6
    header("Q6")
    with t.measure("Q6"):
        q6 = (df.groupby("country", observed=True)
                .agg(registros=("geonameid", "count"),
                     poblacion_total=("poblacion_asentamiento", "sum"),
                     elevacion_promedio=("elevacion_final", "mean"))
                .compute()
                .round({"elevacion_promedio": 2})
                .sort_values("poblacion_total", ascending=False)
                .reset_index())
    save_result(q6, out, "Q6")

    # ------------------------------------------------------------------ Q7
    header("Q7")
    with t.measure("Q7"):
        total = len(df)
        q7 = (df.groupby(["fclass", "fclass_desc"], dropna=False, observed=True).size()
                .compute().reset_index(name="registros")
                .sort_values("registros", ascending=False))
        q7["porcentaje"] = (q7["registros"] / total * 100).round(2)
    save_result(q7, out, "Q7")

    # ------------------------------------------------------------------ Q8
    header("Q8")
    with t.measure("Q8"):
        q8 = (df[(df["fclass"] == "P") & (df["population"] > 1_000_000)]
                .groupby("country", observed=True)
                .agg(ciudades_mas_1M=("geonameid", "count"),
                     poblacion_en_esas_ciudades=("population", "sum"))
                .compute()
                .sort_values(["ciudades_mas_1M", "poblacion_en_esas_ciudades"], ascending=False)
                .reset_index())
    save_result(q8, out, "Q8")

    # ------------------------------------------------------------------ Q9
    header("Q9")
    with t.measure("Q9"):
        sa = df[df["country"].isin(SOUTH_AMERICA) & (df["fclass"] == "P")] \
               [["country", "name", "population"]]
        q9 = (sa.groupby("country", observed=True)
                .apply(lambda g: g.nlargest(3, "population"), include_groups=False,
                       meta={"name": "object", "population": "int64"})
                .compute()
                .reset_index(level=0)
                .reset_index(drop=True))
        q9["country"] = q9["country"].astype(str)
        q9["ranking"] = q9.groupby("country")["population"].rank(method="first",
                                                                 ascending=False).astype(int)
        q9 = q9.sort_values(["country", "ranking"])[["country", "ranking", "name", "population"]]
    save_result(q9, out, "Q9", show=36)

    # ------------------------------------------------------------------ Q10
    header("Q10")
    with t.measure("Q10"):
        q10 = (df.groupby("anio_mod").size().compute()
                 .sort_index().reset_index(name="registros"))
        q10["anio_mod"] = q10["anio_mod"].astype(int)
        q10["pct_acumulado"] = (q10["registros"].cumsum() / q10["registros"].sum() * 100).round(2)
    save_result(q10, out, "Q10", show=40)

    if write_processed:
        with t.measure("escritura_parquet"):
            df.to_parquet(write_processed, write_index=False)

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
