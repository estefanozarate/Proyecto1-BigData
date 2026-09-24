#!/usr/bin/env bash
# =============================================================================
# Hadoop MapReduce en Google Cloud Dataproc (hadoop-mapreduce-examples.jar)
#
# Ejecutar DENTRO del nodo master del clúster:
#   gcloud compute ssh cluster-proyecto-bigdata-m --zone=us-east4-b --tunnel-through-iap
#   (el clúster usa solo IPs internas, por eso se entra por IAP; también sirve
#    el botón SSH de la consola en Compute Engine > Instancias de VM)
#   Subir antes esta carpeta al master:
#   gcloud compute scp --recurse mapreduce cluster-proyecto-bigdata-m:~ \
#        --zone=us-east4-b --tunnel-through-iap
#
# Programas elegidos (no se usa wordcount ni grep):
#   1. wordmean       -> longitud media de las palabras de los nombres geográficos
#   2. wordmedian     -> longitud mediana de esas palabras
#   3. secondarysort  -> elevaciones ordenadas dentro de cada franja de latitud
#   (extra) terasort  -> ordenamiento distribuido teragen/terasort/teravalidate
#
# Tomar captura de cada bloque (comando + salida) para el informe.
# =============================================================================
set -u   # sin -e: los "| head" cortan tuberías y no deben abortar el script
BUCKET="utec-bigdata-geonames-2026"
JAR=/usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar
IN=/data/input
OUT=/data/output

# --- 0. Lista de programas disponibles en el JAR -----------------------------
hadoop jar $JAR 2>&1 | head -40

# --- 1. Preparar entradas y cargarlas a HDFS ---------------------------------
cd ~/mapreduce
gcloud storage cat "gs://$BUCKET/raw/allCountries_headers.csv" | python3 preparar_entradas.py

hdfs dfs -mkdir -p $IN/nombres $IN/secondarysort $OUT
hdfs dfs -put -f nombres.txt  $IN/nombres/
hdfs dfs -put -f lat_dem.txt  $IN/secondarysort/
# (también se sube el CSV crudo completo a HDFS, desde el Data Lake)
hadoop distcp -overwrite "gs://$BUCKET/raw/allCountries_headers.csv" $IN/raw/

hdfs dfs -ls -R $IN
hdfs dfs -du -h $IN
hdfs fsck $IN -files -blocks | tail -20       # bloques y réplicas en los DataNodes

hdfs dfs -rm -r -f $OUT/wordmean $OUT/wordmedian $OUT/secondarysort

# --- 2. Programa 1: wordmean -------------------------------------------------
time hadoop jar $JAR wordmean $IN/nombres $OUT/wordmean
hdfs dfs -ls $OUT/wordmean
hdfs dfs -cat $OUT/wordmean/part-r-00000      # count = nº de palabras, length = suma de longitudes

# --- 3. Programa 2: wordmedian -----------------------------------------------
time hadoop jar $JAR wordmedian $IN/nombres $OUT/wordmedian
hdfs dfs -ls $OUT/wordmedian
hdfs dfs -cat $OUT/wordmedian/part-r-00000 | sort -n | head -30   # histograma longitud -> frecuencia

# --- 4. Programa 3: secondarysort --------------------------------------------
time hadoop jar $JAR secondarysort $IN/secondarysort $OUT/secondarysort
hdfs dfs -ls $OUT/secondarysort
hdfs dfs -du -h $OUT/secondarysort
# Franja de latitud -12 (Lima): elevaciones ordenadas de menor a mayor
hdfs dfs -cat "$OUT/secondarysort/part-r-*" | awk -F'\t' '$1=="-12"' | head -5
hdfs dfs -cat "$OUT/secondarysort/part-r-*" | awk -F'\t' '$1=="-12"' | tail -5
# Elevación máxima por franja (último valor de cada grupo ordenado)
hdfs dfs -cat "$OUT/secondarysort/part-r-*" | grep -v '^---' \
  | awk -F'\t' '{max[$1]=$2} END {for (k in max) print k"\t"max[k]}' | sort -k2 -n -r | head -10

# --- 5. (Extra) terasort ------------------------------------------------------
hdfs dfs -rm -r -f $OUT/teragen $OUT/terasort $OUT/teravalidate
time hadoop jar $JAR teragen 10000000 $OUT/teragen          # 10M filas de 100 bytes = 1 GB
time hadoop jar $JAR terasort $OUT/teragen $OUT/terasort
time hadoop jar $JAR teravalidate $OUT/terasort $OUT/teravalidate
hdfs dfs -cat $OUT/teravalidate/part-r-00000                 # "checksum" = orden correcto

# --- 6. Persistir resultados en el Data Lake ---------------------------------
hadoop distcp -overwrite $OUT/wordmean $OUT/wordmedian $OUT/secondarysort \
    "gs://$BUCKET/mapreduce/output/"
gcloud storage ls -r "gs://$BUCKET/mapreduce/"

# Historial de jobs (también visible en la consola de Dataproc > Jobs / YARN UI)
mapred job -list all | tail -10
