#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, lit
from graphframes import GraphFrame

def init_spark_session():
    """Initialise la SparkSession de manière isolée."""
    return SparkSession.builder \
        .appName("LeBonCoinStreamingGraph") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def get_event_schema():
    """Retourne le schéma strict pour valider les données entrantes."""
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


def extraire_sommets(df_batch):
    """
    Fonction Pure : Transforme le DataFrame brut en un DataFrame de sommets uniques.
    Modélisation : [id, type, label]
    """
    users = df_batch.select(col("user_id").alias("id"), lit("U").alias("type"), col("user_city").alias("label"))
    sellers = df_batch.select(col("seller_id").alias("id"), lit("S").alias("type"), lit("Vendeur").alias("label"))
    products = df_batch.select(col("product_id").alias("id"), lit("P").alias("type"), col("product_cat").alias("label"))
    
    # Union et suppression des doublons
    return users.union(sellers).union(products).distinct()


def extraire_aretes(df_batch):
    """
    Fonction Pure : Transforme le DataFrame brut en un DataFrame d'arêtes.
    Modélisation : [src, dst, relationship]
    """
    # Liens : Utilisateurs -> Produits (Actions : AIME, ACHAT, etc.)
    user_to_prod = df_batch.select(col("user_id").alias("src"), col("product_id").alias("dst"), col("action_type").alias("relationship"))
    
    # Liens : Vendeurs -> Produits
    seller_to_prod = df_batch.select(col("seller_id").alias("src"), col("product_id").alias("dst"), lit("PROPOSE").alias("relationship"))
    
    return user_to_prod.union(seller_to_prod).distinct()


def calculer_top_degres(graph, n=5):
    """Fonction Pure : Calcule les n nœuds les plus connectés (Activité globale)."""
    return graph.degrees.orderBy(col("degree").desc()).limit(n)


def calculer_top_produits_populaires(graph, n=3):
    """Fonction Pure : Calcule les n produits ayant le plus de liens entrants (In-Degree)."""
    return graph.inDegrees.filter("id LIKE 'prod_%'").orderBy(col("inDegree").desc()).limit(n)


def sauvegarder_donnees(vertices_df, edges_df, chemin_base="./data/dashboard"):
    """Effet de bord : Persiste les sommets et les arêtes au format JSON."""
    try:
        vertices_df.write.mode("append").json(f"{chemin_base}/vertices")
        edges_df.write.mode("append").json(f"{chemin_base}/edges")
    except Exception as e:
        print(f"[ERREUR EXPORT] Impossible de sauvegarder les données : {e}")


def afficher_metriques(batch_id, df_degres, df_produits):
    """Effet de bord : Centralise tous les affichages du Micro-Batch dans la console."""
    print(f"\n" + "="*50)
    print(f" TRAITEMENT DU MICRO-BATCH # {batch_id} ")
    print("="*50)
    
    print("\n[MÉTRIQUE GRAPHE] Top 5 des nœuds les plus connectés (Degrés) :")
    df_degres.show(truncate=False)
    
    print("[MÉTRIQUE GRAPHE] Top 3 des Produits les plus populaires (In-Degree) :")
    df_produits.show(truncate=False)


def creer_processeur_de_batch():
    """
    Retourne une fonction de traitement (fermeture / closure).
    Cette structure permet d'encapsuler la logique sans variables globales.
    """
    def processeur(df_batch, batch_id):
        if df_batch.isEmpty():
            return

        
        vertices_df = extraire_sommets(df_batch)
        edges_df = extraire_aretes(df_batch)
        g = GraphFrame(vertices_df, edges_df)

        top_degres = calculer_top_degres(g)
        top_produits = calculer_top_produits_populaires(g)

        afficher_metriques(batch_id, top_degres, top_produits)
        sauvegarder_donnees(vertices_df, edges_df)

    return processeur


def main():
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Connexion réseau au simulateur
    raw_stream = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9990) \
        .load()

    parsed_stream = raw_stream \
        .select(from_json(col("value").cast("string"), get_event_schema()).alias("data")) \
        .select("data.*")

    query = parsed_stream.writeStream \
        .foreachBatch(creer_processeur_de_batch()) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()