#!/bin/bash

echo "=== [INITIALISATION] Préparation de l'environnement ==="

# 1. Nettoyage des ports
echo "Arrêt des processus sur le port 9990 (Simulateur)..."
fuser -k 9990/tcp 2>/dev/null
echo "Arrêt des processus sur le port 8501 (Streamlit)..."
fuser -k 8501/tcp 2>/dev/null

# 2. Configuration Java 17 et correctifs de sécurité JVM pour Spark 4+
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_SUBMIT_OPTS="-Djdk.security.allowCustomAndSecContext=true"

# Détection de l'exécutable Python du venv (ou fallback sur le système)
if [ -f "venv/bin/python" ]; then
    PYTHON_EXEC="venv/bin/python"
else
    PYTHON_EXEC="python3"
fi

# 3. Lancement du simulateur de données
echo -e "\n=== [1/4] Lancement du simulateur.py ==="
$PYTHON_EXEC simulateur.py &
SIM_PID=$!

# Attendre que le serveur socket soit initialisé
sleep 2

# 4. Lancement de l'Analyseur PySpark
echo -e "\n=== [2/4] Lancement de l'analyseur PySpark ==="
spark-submit \
  --packages io.graphframes:graphframes-spark4_2.13:0.11.0 \
  --driver-java-options "-Dsun.security.jgss.native=true --add-opens=java.base/javax.security.auth=ALL-UNNAMED" \
  analyseur.py &
SPARK_PID=$!

# 5. Lancement du Dashboard Streamlit
echo -e "\n=== [3/4] Lancement du Dashboard Streamlit ==="
$PYTHON_EXEC -m streamlit run dashboard_app.py --server.port 8501 --server.headless true &
STREAMLIT_PID=$!

# Attendre un instant que Streamlit démarre
sleep 2

# --- GESTION DE L'ARRÊT GLOBAL ---
# Cette section s'exécute dès que l'utilisateur ferme la fenêtre Tkinter ou fait un Ctrl+C
cleanup() {
    echo -e "\n\n=== [ARRÊT] Fermeture de tous les composants en arrière-plan ==="
    
    echo "Arrêt du simulateur (PID: $SIM_PID)..."
    kill $SIM_PID 2>/dev/null
    
    echo "Arrêt de PySpark (PID: $SPARK_PID)..."
    kill $SPARK_PID 2>/dev/null
    
    echo "Arrêt de Streamlit (PID: $STREAMLIT_PID)..."
    kill $STREAMLIT_PID 2>/dev/null
    
    echo "Nettoyage final des ports..."
    fuser -k 9990/tcp 8501/tcp 2>/dev/null
    
    echo "Tout est arrêté avec succès."
}

# Lie le signal de sortie (fin du script Tkinter ou interruption utilisateur) à la fonction de nettoyage
trap cleanup EXIT