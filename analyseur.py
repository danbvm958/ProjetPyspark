#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Projet d'Ingénierie Big Data & Analyse de Graphes Temps Réel
Module : Plateforme de Streaming Infini d'Interactions Commerciales
Fichier : analyseur.py (Pipeline PySpark Structured Streaming & GraphFrames)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, lit, window
from graphframes import GraphFrame

def init_spark_session():
    """Initialise la SparkSession avec des configurations optimisées."""
    return SparkSession.builder \
        .appName("LeBonCoinStreamingGraph") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def get_event_schema():
    """Définit le schéma strict pour le Schema Enforcement."""
    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_city", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_cat", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("action_type", StringType(), True),
        StructField("price", DoubleType(), True)
    ])

def process_batch(df_batch, batch_id):
    if df_batch.isEmpty():
        return

    print(f"\n" + "="*50)
    print(f" TRAITEMENT DU MICRO-BATCH # {batch_id} ")
    print("="*50)

    # 1. MODÉLISATION DES SOMMETS : [id, type, label]

    # Entités Utilisateurs (U)
    users_vertices = df_batch.select(
        col("user_id").alias("id"),
        lit("U").alias("type"),
        col("user_city").alias("label")
    ).distinct()

    # Entités Vendeurs (S)
    sellers_vertices = df_batch.select(
        col("seller_id").alias("id"),
        lit("S").alias("type"),
        lit("Vendeur").alias("label")
    ).distinct()

    # Entités Produits (P)
    products_vertices = df_batch.select(
        col("product_id").alias("id"),
        lit("P").alias("type"),
        col("product_cat").alias("label")
    ).distinct()

    # Union de tous les sommets uniques du micro-batch
    vertices_df = users_vertices.union(sellers_vertices).union(products_vertices).distinct()

    # 2. MODÉLISATION DES ARÊTES : [src, dst, relationship]

    # Liens : Utilisateurs -(AIME/VOUT/ACHAT)-> Produits
    user_to_product_edges = df_batch.select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        col("action_type").alias("relationship")
    )

    # Liens : Vendeurs -(PROPOSE)-> Produits
    seller_to_product_edges = df_batch.select(
        col("seller_id").alias("src"),
        col("product_id").alias("dst"),
        lit("PROPOSE").alias("relationship")
    ).distinct()

    # Union des arêtes
    edges_df = user_to_product_edges.union(seller_to_product_edges)

    # 3. INITIALISATION ET CALCULS GRAPHFRAMES
    
    g = GraphFrame(vertices_df, edges_df)

    # Indicateur A : Évolution des degrés des nœuds (Popularité / Activité)
    print("\n[MÉTRIQUE GRAPHE] Top 5 des nœuds les plus connectés (Degrés) :")
    g.degrees.orderBy(col("degree").desc()).show(5, truncate=False)

    # Indicateur B : Centralité In-Degree (Spécifique aux Produits les plus ciblés)
    print("[MÉTRIQUE GRAPHE] Top 3 des Produits les plus consultés/achetés (In-Degree) :")
    g.inDegrees.filter("id LIKE 'prod_%'").orderBy(col("inDegree").desc()).show(3, truncate=False)


    # 4. EXPORT POUR LE DASHBOARD VISUEL (Mode Persistance)

    # Sauvegarde des états du graphe courant pour lecture par le Dashboard
    try:
        vertices_df.write.mode("append").json("./data/dashboard/vertices")
        edges_df.write.mode("append").json("./data/dashboard/edges")
    except Exception as e:
        print(f"[ERREUR EXPORT DASHBOARD] {e}")


def main():
    # Initialisation de l'environnement Spark
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Définition du schéma d'entrée
    event_schema = get_event_schema()

    # Connexion au flux de streaming via le Socket TCP
    raw_stream = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9990) \
        .load()

    # Désérialisation et Schema Enforcement
    parsed_stream = raw_stream \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*")

    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()