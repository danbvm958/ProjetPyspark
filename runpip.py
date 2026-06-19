import os
import sys
import time
import subprocess
import shutil
import importlib
import webbrowser

PORT = 9990

def kill_process_on_port(port):
    """Tue proprement le processus qui occupe le port spécifié (Linux & Windows)."""
    print(f"--- Nettoyage du port {port} ---")
    if sys.platform.startswith('win'):
        # Commande Windows pour trouver le PID sur le port
        try:
            cmd = f'netstat -ano | findstr :{port}'
            output = subprocess.check_output(cmd, shell=True).decode()
            for line in output.strip().split('\n'):
                if 'LISTENING' in line or 'ESTABLISHED' in line:
                    pid = line.strip().split()[-1]
                    print(f"Windows: Fermeture du processus PID {pid}")
                    subprocess.run(f"taskkill /F /PID {pid}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            print("Aucun processus actif sur ce port.")
    else:
        # Commande Linux/Mac
        print("Linux: Libération du port via fuser/kill...")
        subprocess.run(f"fuser -k {port}/tcp", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def setup_java_env():
    """Configure dynamiquement Java, Spark PIP et Hadoop Local."""
    if sys.platform.startswith('win'):
        print("--- Configuration de l'environnement Windows (PIP Spark) ---")
        
        # 1. Chemin JDK Java
        os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17" 
        
        # 2. Configurer HADOOP_HOME dynamiquement par rapport au projet
        # Récupère le dossier racine du projet (où se trouve ce script)
        project_root = os.path.dirname(os.path.abspath(__file__))
        os.environ["HADOOP_HOME"] = os.path.join(project_root, "hadoop")
        
        # 3. Trouver le Spark de PIP
        venv_root = os.path.dirname(os.path.dirname(sys.executable))
        pip_spark_home = os.path.join(venv_root, "Lib", "site-packages", "pyspark")
        os.environ["SPARK_HOME"] = pip_spark_home
        
        # 4. Configurer Python pour Spark
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        # 5. Mettre à jour le PATH complet
        spark_bin = os.path.join(pip_spark_home, "bin")
        java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
        hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
        
        os.environ["PATH"] = f"{spark_bin};{java_bin};{hadoop_bin};{os.environ['PATH']}"
        
        print(f"[OK] JAVA_HOME : {os.environ['JAVA_HOME']}")
        print(f"[OK] HADOOP_HOME local : {os.environ['HADOOP_HOME']}")
        print(f"[OK] SPARK_HOME détecté dans venv : {os.environ['SPARK_HOME']}")

def get_spark_submit_path():
    try:
        pyspark = importlib.import_module("pyspark")
        spark_pkg = os.path.dirname(pyspark.__file__)
        candidates = [
            os.path.join(spark_pkg, "bin", "spark-submit.cmd"),
            os.path.join(spark_pkg, "bin", "spark-submit.bat"),
            os.path.join(spark_pkg, "bin", "spark-submit"),
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
    except Exception:
        pass
    return shutil.which("spark-submit")

def main():
    # 1. Libérer le port 9990
    kill_process_on_port(PORT)
    time.sleep(1)

    # 2. Configurer l'environnement
    setup_java_env()

    # 3. Lancer le simulateur en arrière-plan
    print("\n--- Lancement du simulateur.py ---")
    sim_proc = subprocess.Popen([sys.executable, "simulateur.py"], env=os.environ.copy())
    
    # Pause pour laisser le socket se lier (bind)
    time.sleep(2)

    # 4. Lancer le dashboard moderne (Streamlit) en premier si disponible
    STREAMLIT_PORT = 8501
    st_proc = None
    try:
        import streamlit  # type: ignore
        print("[INFO] Streamlit détecté : lancement du dashboard moderne via streamlit run")
        streamlit_cmd = [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "dashboard_app.py",
            "--server.port",
            str(STREAMLIT_PORT),
            "--server.headless",
            "true",
        ]
        st_proc = subprocess.Popen(streamlit_cmd, env=os.environ.copy(), shell=False)
        # Ouvre automatiquement le navigateur sur l'URL du dashboard
        try:
            url = f"http://localhost:{STREAMLIT_PORT}"
            time.sleep(1.5)
            webbrowser.open_new_tab(url)
        except Exception:
            pass
    except Exception:
        st_proc = None

    # 5. Préparer et lancer Spark-Submit (optionnel) — ne bloque pas le dashboard si inexistant
    spark_submit_path = get_spark_submit_path()
    if not spark_submit_path or not os.path.exists(spark_submit_path):
        print("[WARN] spark-submit introuvable. Le pipeline Spark ne sera pas lancé automatiquement.")
    else:
        spark_cmd = [
            spark_submit_path,
            "--packages", "io.graphframes:graphframes-spark4_2.13:0.11.0",
            "--driver-java-options", "-Dsun.security.jgss.native=true --add-opens=java.base/javax.security.auth=ALL-UNNAMED",
            "analyseur.py"
        ]
        try:
            print("\n--- Lancement de l'analyseur PySpark (spark-submit) ---")
            spark_proc = subprocess.Popen(spark_cmd, env=os.environ.copy(), shell=False)
        except Exception as e:
            print(f"[ERREUR] Échec du lancement Spark: {e}")

if __name__ == "__main__":
    main()
