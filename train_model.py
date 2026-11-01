import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

LABELS = ["Data", "Développement", "Sécurité", "Cloud"]

spark = (SparkSession.builder
         .appName("TrainJobClassifier")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Lecture des données d'entraînement
df = (spark.read.option("header", True).option("sep", ";")
      .option("encoding", "UTF-8").csv("data/train.csv"))

# 2. Label texte -> indice numérique (0.0, 1.0, 2.0, 3.0)
mapping = create_map([lit(x) for x in chain(*[(l, float(i)) for i, l in enumerate(LABELS)])])
df = (df.withColumn("label", mapping[col("label")])
        .withColumnRenamed("texte", "offre")
        .dropna())
print("Nombre d'exemples :", df.count())
df.groupBy("label").count().orderBy("label").show()

# 3. Séparation entraînement / test
train, test = df.randomSplit([0.8, 0.2], seed=42)

# 4. Pipeline
tokenizer = RegexTokenizer(inputCol="offre", outputCol="mots",
                           pattern="[^\\p{L}\\p{N}]+", toLowercase=True)
remover = StopWordsRemover(
    inputCol="mots", outputCol="mots_utiles",
    stopWords=StopWordsRemover.loadDefaultStopWords("french")
              + StopWordsRemover.loadDefaultStopWords("english"))
tf = HashingTF(inputCol="mots_utiles", outputCol="tf", numFeatures=2048)
idf = IDF(inputCol="tf", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label",
                        maxIter=50, regParam=0.01)

pipeline = Pipeline(stages=[tokenizer, remover, tf, idf, lr])

# 5. Entraînement
model = pipeline.fit(train)

# 6. Évaluation
pred = model.transform(test)
for metric in ["accuracy", "f1"]:
    ev = MulticlassClassificationEvaluator(labelCol="label",
                                           predictionCol="prediction",
                                           metricName=metric)
    print(f"{metric} = {ev.evaluate(pred):.3f}")

print("Matrice de confusion (label réel, prédiction, effectif) :")
pred.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

# 7. Sauvegarde
model.write().overwrite().save("models/job_classifier")
print("Modèle sauvegardé dans models/job_classifier")
spark.stop()