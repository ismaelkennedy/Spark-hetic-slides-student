"""Pipeline MovieLens — Projet Jour 4.

Architecture :
    data/raw/  (bronze)  ->  data/silver/  (Parquet)  ->  data/gold/  (résultats)

Lancement depuis la racine du projet :
    python projects/movielens/pipeline.py
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, StringType
from pyspark.sql.window import Window

from spark_session import get_spark

# Chemins
RATINGS_CSV  = "data/raw/ml-latest-small/ratings.csv"
MOVIES_CSV   = "data/raw/ml-latest-small/movies.csv"
SORTIE_SILVER = "data/silver/ratings"
SORTIE_GOLD   = "data/gold"


def ingestion(spark):
    """Étape 1a : lire les CSV bruts avec schéma explicite."""

    schema_ratings = StructType([
        StructField("userId",    IntegerType(), False),
        StructField("movieId",   IntegerType(), False),
        StructField("rating",    FloatType(),   False),
        StructField("timestamp", LongType(),    False),
    ])

    schema_movies = StructType([
        StructField("movieId", IntegerType(), False),
        StructField("title",   StringType(),  False),
        StructField("genres",  StringType(),  False),
    ])

    ratings = spark.read.csv(RATINGS_CSV, header=True, schema=schema_ratings)
    movies  = spark.read.csv(MOVIES_CSV,  header=True, schema=schema_movies)

    ratings.printSchema()
    print("Lignes ratings brutes :", ratings.count())
    print("Lignes movies brutes  :", movies.count())

    return ratings, movies


def nettoyage(ratings, movies):
    """Étape 1b : nettoyer les données (bronze -> silver)."""

    avant = ratings.count()

    # Timestamp Unix → date lisible + année pour le partitionnement silver
    ratings = ratings.withColumn("date", F.to_date(F.from_unixtime(F.col("timestamp"))))
    ratings = ratings.withColumn("year", F.year(F.col("date")))
    ratings = ratings.drop("timestamp")

    # Doublons
    ratings = ratings.dropDuplicates()

    # Notes hors bornes (MovieLens : 0.5 à 5.0 par pas de 0.5)
    ratings = ratings.filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0))

    # Valeurs manquantes
    ratings = ratings.na.drop()

    apres = ratings.count()
    print(f"Lignes brutes    : {avant}")
    print(f"Lignes nettoyées : {apres}")
    print(f"Lignes écartées  : {avant - apres} ({round((avant - apres) / avant * 100, 2)} %)")

    return ratings


def ecrire_silver(ratings_clean):
    """Étape 1c : écrire la couche silver en Parquet partitionnée par année."""
    ratings_clean.write.mode("overwrite").partitionBy("year").parquet(SORTIE_SILVER)
    print("Silver écrite dans", SORTIE_SILVER)


def transformation_et_analyses(spark, movies):
    """Étape 2 : 3 analyses depuis la silver."""

    df = spark.read.parquet(SORTIE_SILVER)

    # TODO  : analyses 1, 2, 3
    raise NotImplementedError("TODO analyses")


def ecrire_gold(resultats):
    """Étape 3 : écrire les résultats agrégés."""
    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"
        df.coalesce(1).write.mode("overwrite").parquet(chemin)
        print("Gold écrit :", chemin)


def main():
    spark = get_spark("Projet MovieLens")
    print("Spark UI : http://localhost:4040")

    ratings, movies = ingestion(spark)
    ratings_clean = nettoyage(ratings, movies)
    ecrire_silver(ratings_clean)

    resultats = transformation_et_analyses(spark, movies)
    ecrire_gold(resultats)

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except NotImplementedError as e:
        print("\nPipeline incomplet :", e)
        sys.exit(1)