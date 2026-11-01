import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, array_max, round as sround
from pyspark.ml import PipelineModel
from pyspark.ml.feature import IndexToString
from pyspark.ml.functions import vector_to_array

LABELS = ["Data", "Développement", "Sécurité", "Cloud"]

spark = (SparkSession.builder
         .appName("JobClassifierL2")
         .master("local[2]")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Chargement du modèle entraîné
model = PipelineModel.load("models/job_classifier")

# 2. Lecture du flux Kafka
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "job-offers")
       .option("startingOffsets", "latest")
       .load())

offres = raw.selectExpr("CAST(value AS STRING) AS offre")

# 3. Prédiction Level 2 (MLlib)
pred = model.transform(offres)
pred = IndexToString(inputCol="prediction", outputCol="domaine_L2",
                     labels=LABELS).transform(pred)

# 4. Règles Level 1 (pour comparaison)
txt = lower(col("offre"))
pred = pred.withColumn(
    "domaine_L1",
    when(txt.rlike(r"sécurité|securite|cyber|pentest|\bsoc\b|siem|intrusion|vulnérabilit|rssi|iso 27001"), "Sécurité")
    .when(txt.rlike(r"\baws\b|azure|gcp|kubernetes|docker|devops|cloud|terraform"), "Cloud")
    .when(txt.rlike(r"\bdata\b|spark|sql|machine learning|\betl\b|power bi|\bbi\b|hadoop"), "Data")
    .when(txt.rlike(r"développeur|developpeur|java|react|angular|php|django|node|full stack|\.net|flutter"), "Développement")
    .otherwise("Autre")
)

# 5. Colonnes affichées
result = pred.select(
    "offre",
    "domaine_L1",
    "domaine_L2",
    sround(array_max(vector_to_array(col("probability"))), 2).alias("confiance"),
)

query = (result.writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .start())

query.awaitTermination()