#!/bin/bash

echo "Arrêt des processus sur le port 9990..."
fuser -k 9990/tcp 2>/dev/null

# On force l'utilisation de Java 17 pour Spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

echo "Lancement du simulateur..."
python simulateur.py &
SIM_PID=$!

# Attendre 2 secondes que le serveur python soit bien initialisé
sleep 2

echo "Lancement de l'analyseur PySpark..."
spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 analyseur.py

# Gestion de l'arrêt complet
trap "kill $SIM_PID" EXIT