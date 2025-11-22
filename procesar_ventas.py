from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Asegurar UTF-8
try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

spark = SparkSession.builder \
    .appName("Laboratorio Ventas Spark") \
    .getOrCreate()

df = spark.read.csv("/app/hecho_ventas.csv", header=True, inferSchema=True)

df = df.withColumn("total", col("cantidad") * col("precio_unitario"))

df_ciudad = df.groupBy("ciudad_cliente").sum("total")

print("\n=== RESULTADO ===")
for row in df_ciudad.collect():
    print(row.asDict())

df_ciudad.toPandas().to_csv("resultado_ciudad.csv", index=False, encoding="utf-8")

print("\nArchivo generado: resultado_ciudad.csv")
