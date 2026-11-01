import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, array_max
from pyspark.ml import PipelineModel
from pyspark.ml.feature import IndexToString
from pyspark.ml.functions import vector_to_array
from elasticsearch import Elasticsearch, helpers

LABELS = ["Data", "Développement", "Sécurité", "Cloud"]
INDEX = "job-offers-classified"

# ---------- Elasticsearch ----------
es = Elasticsearch("http://localhost:9200")

if not es.indices.exists(index=INDEX):
    es.indices.create(
        index=INDEX,
        mappings={"properties": {
            "offre":      {"type": "text",
                           "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "domaine":    {"type": "keyword"},   # prédiction Level 2 (MLlib)
            "domaine_L1": {"type": "keyword"},   # prédiction Level 1 (mots-clés)
            "confiance":  {"type": "float"},
            "ts":         {"type": "date"},
        }},
    )
    print("Index créé :", INDEX)

# ---------- Spark ----------
spark = (SparkSession.builder
         .appName("JobClassifierL3")
         .master("local[2]")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

model = PipelineModel.load("models/job_classifier")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "job-offers")
       .option("startingOffsets", "latest")
       .load())

offres = raw.selectExpr("CAST(value AS STRING) AS offre")

# Prédiction MLlib (Level 2)
pred = model.transform(offres)
pred = IndexToString(inputCol="prediction", outputCol="domaine",
                     labels=LABELS).transform(pred)

# Règles mots-clés (Level 1), pour comparer
txt = lower(col("offre"))
pred = pred.withColumn(
    "domaine_L1",
    when(txt.rlike(r"sécurité|securite|cyber|pentest|\bsoc\b|siem|intrusion|vulnérabilit|rssi|iso 27001"), "Sécurité")
    .when(txt.rlike(r"\baws\b|azure|gcp|kubernetes|docker|devops|cloud|terraform"), "Cloud")
    .when(txt.rlike(r"\bdata\b|spark|sql|machine learning|\betl\b|power bi|\bbi\b|hadoop"), "Data")
    .when(txt.rlike(r"développeur|developpeur|java|react|angular|php|django|node|full stack|\.net|flutter"), "Développement")
    .otherwise("Autre")
)

result = pred.select(
    "offre",
    "domaine",
    "domaine_L1",
    array_max(vector_to_array(col("probability"))).alias("confiance"),
)

# ---------- Envoi vers Elasticsearch ----------
def send_to_es(batch_df, batch_id):
    rows = batch_df.collect()
    now = datetime.now(timezone.utc).isoformat()
    actions = [{
        "_index": INDEX,
        "_source": {
            "offre": r["offre"],
            "domaine": r["domaine"],
            "domaine_L1": r["domaine_L1"],
            "confiance": float(r["confiance"]),
            "ts": now,
        },
    } for r in rows]
    if actions:
        helpers.bulk(es, actions)
    print(f"Batch {batch_id} : {len(actions)} offre(s) envoyée(s) à Elasticsearch")
    for r in rows:
        print(f"   [{r['domaine']:<13}] {r['offre']}")

query = (result.writeStream
         .foreachBatch(send_to_es)
         .option("checkpointLocation", "checkpoints/l3")
         .start())

query.awaitTermination()