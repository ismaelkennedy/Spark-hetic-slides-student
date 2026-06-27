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
    df = df.cache()
    df.count()

    # --- Analyse 1 : films les mieux notés (agrégation) ---
    analyse_1 = (
        df.groupBy("movieId")
        .agg(
            F.avg("rating").alias("note_moyenne"),
            F.count("rating").alias("nb_votes"),
        )
        .filter(F.col("nb_votes") >= 50)
        .orderBy(F.desc("note_moyenne"))
    )
    print("=== Analyse 1 — Top films (min 50 votes) ===")
    analyse_1.show(10)

    # --- Analyse 2 : jointure ratings + movies (broadcast) ---
    analyse_2 = (
        df.join(F.broadcast(movies), on="movieId", how="inner")
        .groupBy("movieId", "title", "genres")
        .agg(
            F.avg("rating").alias("note_moyenne"),
            F.count("rating").alias("nb_votes"),
        )
        .filter(F.col("nb_votes") >= 50)
        .orderBy(F.desc("note_moyenne"))
    )
    print("=== Analyse 2 — Top films avec titres ===")
    analyse_2.show(10)

    # --- Analyse 3 : classement par genre (window function) ---
    df_genres = analyse_2.withColumn(
        "genre", F.explode(F.split(F.col("genres"), "\\|"))
    )
    fenetre = Window.partitionBy("genre").orderBy(F.desc("note_moyenne"))
    analyse_3 = (
        df_genres.withColumn("rang", F.row_number().over(fenetre))
        .filter(F.col("rang") <= 5)
        .select("genre", "rang", "title", "note_moyenne", "nb_votes")
        .orderBy("genre", "rang")
    )
    print("=== Analyse 3 — Top 5 par genre ===")
    analyse_3.show(20)

    return {"analyse_1": analyse_1, "analyse_2": analyse_2, "analyse_3": analyse_3}


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