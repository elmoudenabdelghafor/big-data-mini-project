import os
import sys

# Utiliser le Python du venv pour driver et workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when

spark = (SparkSession.builder
         .appName("JobClassifierL1")
         .master("local[2]")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Lecture du flux Kafka
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "job-offers")
       .option("startingOffsets", "latest")
       .load())

# 2. Conversion du message (binaire) en texte
offres = raw.selectExpr("CAST(value AS STRING) AS offre")

# 3. Classification par mots-clés
txt = lower(col("offre"))

classified = offres.withColumn(
    "domaine",
    when(txt.rlike(r"sécurité|securite|cyber|pentest|\bsoc\b|siem|intrusion|vulnérabilit|rssi|iso 27001"), "Sécurité")
    .when(txt.rlike(r"\baws\b|azure|gcp|kubernetes|docker|devops|cloud|terraform"), "Cloud")
    .when(txt.rlike(r"\bdata\b|spark|sql|machine learning|\betl\b|power bi|\bbi\b|hadoop"), "Data")
    .when(txt.rlike(r"développeur|developpeur|java|react|angular|php|django|node|full stack|\.net|flutter"), "Développement")
    .otherwise("Autre")
)

# 4. Affichage dans la console
query = (classified.writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .start())

query.awaitTermination()