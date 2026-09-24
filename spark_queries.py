"""
Procesamiento del dataset GeoNames con Apache Spark (PySpark DataFrames).
"""
import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import (DEFAULT_INPUT, DUP_KEYS, FCLASS_DESC, POP_LABELS, SOUTH_AMERICA,
                    USE_COLUMNS, Timer, header, save_result)

FW = "spark"

SCHEMA = T.StructType([
    T.StructField("geonameid", T.LongType()),
    T.StructField("name", T.StringType()),
    T.StructField("asciiname", T.StringType()),
    T.StructField("alternatenames", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("fclass", T.StringType()),
    T.StructField("fcode", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("cc2", T.StringType()),
    T.StructField("admin1", T.StringType()),
    T.StructField("admin2", T.StringType()),
    T.StructField("admin3", T.StringType()),
    T.StructField("admin4", T.StringType()),
    T.StructField("population", T.LongType()),
    T.StructField("elevation", T.DoubleType()),
    T.StructField("dem", T.LongType()),
    T.StructField("timezone", T.StringType()),
    T.StructField("moddate", T.StringType()),
])


def get_spark():
    return (SparkSession.builder
            .appName("GeoNames Big Data - UTEC")
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.ui.showConsoleProgress", "false")
            # En local/Colab (en Dataproc la memoria la define YARN)
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
            .getOrCreate())


def load(spark, path):
    # Esquema explícito: evita la pasada extra de inferSchema sobre 1.8 GB
    return (spark.read
            .option("header", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .schema(SCHEMA)
            .csv(path)
            .select(*USE_COLUMNS))


def run(path, out_dir, write_processed=None, spark=None):
    spark = spark or get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    t = Timer(FW)
    out = os.path.join(out_dir, FW)

    with t.measure("carga"):
        df = load(spark, path).cache()
        n0 = df.count()
    print(f"Registros leidos: {n0:,} | particiones: {df.rdd.getNumPartitions()}")

    # ------------------------------------------------------------------ Q1
    header("Q1")
    with t.measure("Q1"):
        nulos = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                           for c in df.columns]).toPandas().T.reset_index()
        nulos.columns = ["columna", "nulos"]
        df = (df.withColumn("name", F.trim("name"))
                .withColumn("population", F.coalesce("population", F.lit(0)))
                .withColumn("dem", F.when(F.col("dem") == -9999, None).otherwise(F.col("dem")))
                .withColumn("moddate", F.to_date("moddate", "yyyy-MM-dd")))
    save_result(nulos, out, "Q1", show=20)

    # ------------------------------------------------------------------ Q2
    header("Q2")
    with t.measure("Q2"):
        dup_id = n0 - df.select("geonameid").distinct().count()
        df = df.dropDuplicates(["geonameid"])
        n1 = df.count()
        # Duplicados lógicos: se conserva el menor geonameid de cada grupo
        conservar = df.groupBy(*DUP_KEYS).agg(F.min("geonameid").alias("geonameid"))
        df = df.join(conservar.select("geonameid"), on="geonameid", how="left_semi").cache()
        n2 = df.count()
        import pandas as pd
        q2 = pd.DataFrame({"criterio": ["geonameid", "name+country+lat+lon", "registros_finales"],
                           "valor": [dup_id, n1 - n2, n2]})
    save_result(q2, out, "Q2")

    # ------------------------------------------------------------------ Q3
    header("Q3")
    with t.measure("Q3"):
        reglas = {
            "latitud_fuera_rango": ~F.col("latitude").between(-90, 90),
            "longitud_fuera_rango": ~F.col("longitude").between(-180, 180),
            "poblacion_negativa": F.col("population") < 0,
            "pais_nulo": F.col("country").isNull(),
            "nombre_nulo": F.col("name").isNull(),
        }
        conteos = df.select([F.sum(F.coalesce(e, F.lit(False)).cast("int")).alias(k)
                             for k, e in reglas.items()]).first().asDict()
        invalido = None
        for e in reglas.values():
            e = F.coalesce(e, F.lit(False))
            invalido = e if invalido is None else (invalido | e)
        df = df.filter(~invalido)                            # DELETE
        n3 = df.count()
        q3 = pd.DataFrame({"regla": list(conteos) + ["TOTAL_ELIMINADOS"],
                           "registros": list(conteos.values()) + [n2 - n3]})
    save_result(q3, out, "Q3")

    # ------------------------------------------------------------------ Q4
    header("Q4")
    with t.measure("Q4"):
        mapa = F.create_map([F.lit(x) for kv in FCLASS_DESC.items() for x in kv])
        pop = F.col("population")
        df = (df                                                # CREATE / UPDATE
              .withColumn("anio_mod", F.year("moddate"))
              .withColumn("hemisferio", F.when(F.col("latitude") >= 0, "Norte").otherwise("Sur"))
              .withColumn("elevacion_final", F.coalesce(F.col("elevation"),
                                                        F.col("dem").cast("double")))
              .withColumn("poblacion_asentamiento",
                          F.when(F.col("fclass") == "P", pop).otherwise(F.lit(0)))
              .withColumn("fclass_desc", F.coalesce(mapa[F.col("fclass")], F.lit("Desconocido")))
              .withColumn("categoria_poblacion",
                          F.when(pop <= 0, POP_LABELS[0])
                           .when(pop < 1_000, POP_LABELS[1])
                           .when(pop < 100_000, POP_LABELS[2])
                           .when(pop < 1_000_000, POP_LABELS[3])
                           .otherwise(POP_LABELS[4]))
              .cache())
        q4 = (df.groupBy("categoria_poblacion").agg(F.count("*").alias("registros"))
                .orderBy(F.desc("registros")).toPandas())
    save_result(q4, out, "Q4")

    # ------------------------------------------------------------------ Q5
    header("Q5")
    with t.measure("Q5"):
        pe = df.filter((F.col("country") == "PE") & (F.col("fclass") == "P")
                       & (F.col("population") > 0))
        n_pe = pe.count()
        q5 = (pe.orderBy(F.desc("population"))
                .select("name", "admin1", "population", "elevacion_final",
                        "latitude", "longitude")
                .limit(20).toPandas())
    print(f"Centros poblados con poblacion en Peru: {n_pe:,}")
    save_result(q5, out, "Q5")

    # ------------------------------------------------------------------ Q6
    header("Q6")
    with t.measure("Q6"):
        q6 = (df.groupBy("country")
                .agg(F.count("*").alias("registros"),
                     F.sum("poblacion_asentamiento").alias("poblacion_total"),
                     F.round(F.avg("elevacion_final"), 2).alias("elevacion_promedio"))
                .orderBy(F.desc("poblacion_total")).toPandas())
    save_result(q6, out, "Q6")

    # ------------------------------------------------------------------ Q7
    header("Q7")
    with t.measure("Q7"):
        total = df.count()
        q7 = (df.groupBy("fclass", "fclass_desc").agg(F.count("*").alias("registros"))
                .withColumn("porcentaje", F.round(F.col("registros") / total * 100, 2))
                .orderBy(F.desc("registros")).toPandas())
    save_result(q7, out, "Q7")

    # ------------------------------------------------------------------ Q8
    header("Q8")
    with t.measure("Q8"):
        q8 = (df.filter((F.col("fclass") == "P") & (F.col("population") > 1_000_000))
                .groupBy("country")
                .agg(F.count("*").alias("ciudades_mas_1M"),
                     F.sum("population").alias("poblacion_en_esas_ciudades"))
                .orderBy(F.desc("ciudades_mas_1M"), F.desc("poblacion_en_esas_ciudades"))
                .toPandas())
    save_result(q8, out, "Q8")

    # ------------------------------------------------------------------ Q9
    header("Q9")
    with t.measure("Q9"):
        w = Window.partitionBy("country").orderBy(F.desc("population"))
        q9 = (df.filter(F.col("country").isin(SOUTH_AMERICA) & (F.col("fclass") == "P"))
                .withColumn("ranking", F.row_number().over(w))
                .filter(F.col("ranking") <= 3)
                .select("country", "ranking", "name", "population")
                .orderBy("country", "ranking").toPandas())
    save_result(q9, out, "Q9", show=36)

    # ------------------------------------------------------------------ Q10
    header("Q10")
    with t.measure("Q10"):
        w_acum = Window.orderBy("anio_mod").rowsBetween(Window.unboundedPreceding, 0)
        q10 = (df.filter(F.col("anio_mod").isNotNull())
                 .groupBy("anio_mod").agg(F.count("*").alias("registros"))
                 .withColumn("pct_acumulado",
                             F.round(F.sum("registros").over(w_acum)
                                     / F.sum("registros").over(Window.partitionBy()) * 100, 2))
                 .orderBy("anio_mod").toPandas())
    save_result(q10, out, "Q10", show=40)

    if write_processed:
        with t.measure("escritura_parquet"):
            # Data Lake: capa processed particionada por clase de entidad
            df.write.mode("overwrite").partitionBy("fclass").parquet(write_processed)

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
