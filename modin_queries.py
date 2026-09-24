"""
Uso:
    python src/modin_queries.py --input data/allCountries_headers.csv
    python src/modin_queries.py --input data/allCountries_headers.csv --engine dask
"""
import argparse
import os
import warnings

from common import (DEFAULT_INPUT, DUP_KEYS, FCLASS_DESC, PANDAS_DTYPES, POP_BINS,
                    POP_LABELS, SOUTH_AMERICA, USE_COLUMNS, Timer, header,
                    save_result)

warnings.filterwarnings("ignore")
FW = "modin"


def iniciar_motor(engine):
    os.environ["MODIN_ENGINE"] = engine
    if engine == "ray":
        import ray
        if not ray.is_initialized():
            # Object store acotado para no competir con la memoria del proceso
            ray.init(object_store_memory=int(float(os.environ.get("RAY_OBJECT_STORE_GB", "3.5")) * 1e9), include_dashboard=False,
                     ignore_reinit_error=True, log_to_driver=False)
    import modin.pandas as mpd
    return mpd


def run(path, out_dir, write_processed=None, engine="ray"):
    mpd = iniciar_motor(engine)
    import pandas as pd  # solo para pd.cut / resultados pequeños

    t = Timer(FW)
    out = os.path.join(out_dir, FW)

    with t.measure("carga"):
        df = mpd.read_csv(path, usecols=USE_COLUMNS, dtype=PANDAS_DTYPES,
                          keep_default_na=False, na_values=[""])
    print(f"Registros leidos: {len(df):,} | motor: {engine}")

    # ------------------------------------------------------------------ Q1
    header("Q1")
    with t.measure("Q1"):
        nulos = df.isna().sum()._to_pandas()
        df["name"] = df["name"].str.strip()
        df["population"] = df["population"].fillna(0).astype("int64")
        df["dem"] = df["dem"].mask(df["dem"] == -9999)
        df["moddate"] = mpd.to_datetime(df["moddate"], format="%Y-%m-%d", errors="coerce")
    save_result(nulos.rename_axis("columna").reset_index(name="nulos"), out, "Q1", show=20)

    # ------------------------------------------------------------------ Q2
    header("Q2")
    with t.measure("Q2"):
        n0 = len(df)
        dup_id = int(df.duplicated(subset=["geonameid"]).sum())
        df = df.drop_duplicates(subset=["geonameid"])
        n1 = len(df)
        # Duplicados lógicos (mismo nombre+país+coordenadas): se marcan todas las
        # filas repetidas (keep=False) y se conserva el menor geonameid del grupo
        en_dup = df.loc[df.duplicated(subset=DUP_KEYS, keep=False),
                        DUP_KEYS + ["geonameid"]]._to_pandas()
        minimo = en_dup.groupby(DUP_KEYS, observed=True, dropna=False)["geonameid"].transform("min")
        eliminar = en_dup.loc[en_dup["geonameid"] != minimo, "geonameid"].values
        df = df[~df["geonameid"].isin(eliminar)]
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
        conteos = [int(m.sum()) for m in reglas.values()]
        invalido = reglas["latitud_fuera_rango"]
        for m in list(reglas.values())[1:]:
            invalido = invalido | m
        df = df[~invalido]                                   # DELETE
        q3 = pd.DataFrame({"regla": list(reglas) + ["TOTAL_ELIMINADOS"],
                           "registros": conteos + [n2 - len(df)]})
    save_result(q3, out, "Q3")

    # ------------------------------------------------------------------ Q4
    header("Q4")
    with t.measure("Q4"):
        es_p = df["fclass"] == "P"                           # CREATE / UPDATE
        df["anio_mod"] = df["moddate"].dt.year
        df["hemisferio"] = (df["latitude"] >= 0).map({True: "Norte", False: "Sur"})
        df["elevacion_final"] = df["elevation"].fillna(df["dem"])
        df["poblacion_asentamiento"] = df["population"].where(es_p, 0)
        df["fclass_desc"] = df["fclass"].astype("object").map(FCLASS_DESC).fillna("Desconocido")
        df["categoria_poblacion"] = mpd.cut(df["population"], bins=POP_BINS,
                                            labels=POP_LABELS, right=False).astype(str)
        q4 = (df.groupby("categoria_poblacion").size()
                .sort_values(ascending=False).reset_index(name="registros")._to_pandas())
    save_result(q4, out, "Q4")

    # ------------------------------------------------------------------ Q5
    header("Q5")
    with t.measure("Q5"):
        q5 = (df[(df["country"] == "PE") & (df["fclass"] == "P") & (df["population"] > 0)]
              [["name", "admin1", "population", "elevacion_final", "latitude", "longitude"]]
              .sort_values("population", ascending=False))
        n_pe = len(q5)
        q5 = q5.head(20)._to_pandas()
    print(f"Centros poblados con poblacion en Peru: {n_pe:,}")
    save_result(q5, out, "Q5")

    # ------------------------------------------------------------------ Q6
    header("Q6")
    with t.measure("Q6"):
        q6 = (df.groupby("country", observed=True)
                .agg(registros=("geonameid", "count"),
                     poblacion_total=("poblacion_asentamiento", "sum"),
                     elevacion_promedio=("elevacion_final", "mean"))
                .sort_values("poblacion_total", ascending=False)
                .reset_index()._to_pandas()
                .round({"elevacion_promedio": 2}))
    save_result(q6, out, "Q6")

    # ------------------------------------------------------------------ Q7
    header("Q7")
    with t.measure("Q7"):
        total = len(df)
        q7 = (df.groupby(["fclass", "fclass_desc"], dropna=False, observed=True).size()
                .reset_index(name="registros")
                .sort_values("registros", ascending=False)._to_pandas())
        q7["porcentaje"] = (q7["registros"] / total * 100).round(2)
    save_result(q7, out, "Q7")

    # ------------------------------------------------------------------ Q8
    header("Q8")
    with t.measure("Q8"):
        q8 = (df[(df["fclass"] == "P") & (df["population"] > 1_000_000)]
                .groupby("country", observed=True)
                .agg(ciudades_mas_1M=("geonameid", "count"),
                     poblacion_en_esas_ciudades=("population", "sum"))
                .sort_values(["ciudades_mas_1M", "poblacion_en_esas_ciudades"], ascending=False)
                .reset_index()._to_pandas())
    save_result(q8, out, "Q8")

    # ------------------------------------------------------------------ Q9
    header("Q9")
    with t.measure("Q9"):
        sa = df[df["country"].isin(SOUTH_AMERICA) & (df["fclass"] == "P")] \
               [["country", "name", "population"]].copy()
        sa["country"] = sa["country"].astype(str)
        sa["ranking"] = (sa.groupby("country")["population"]
                           .rank(method="first", ascending=False).astype(int))
        q9 = (sa[sa["ranking"] <= 3].sort_values(["country", "ranking"])
                [["country", "ranking", "name", "population"]]._to_pandas())
    save_result(q9, out, "Q9", show=36)

    # ------------------------------------------------------------------ Q10
    header("Q10")
    with t.measure("Q10"):
        q10 = (df.groupby("anio_mod").size().sort_index()
                 .reset_index(name="registros")._to_pandas())
        q10["anio_mod"] = q10["anio_mod"].astype(int)
        q10["pct_acumulado"] = (q10["registros"].cumsum() / q10["registros"].sum() * 100).round(2)
    save_result(q10, out, "Q10", show=40)

    if write_processed:
        with t.measure("escritura_parquet"):
            df.to_parquet(os.path.join(write_processed, "geonames_clean.parquet"), index=False)

    t.times["total_consultas"] = round(sum(v for k, v in t.times.items()
                                           if k.startswith("Q")), 3)
    print(f"\nTiempos guardados en {t.save(out_dir)}")
    return t.times


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=DEFAULT_INPUT)
    ap.add_argument("--out", default="results")
    ap.add_argument("--write-processed", default=None)
    ap.add_argument("--engine", default=os.environ.get("MODIN_ENGINE", "ray"), choices=["ray", "dask"])
    a = ap.parse_args()
    run(a.input, a.out, a.write_processed, a.engine)
