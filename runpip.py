import os
import sys
import time
import subprocess
import signal

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

    # 4. Préparer la commande Spark-Submit
    # On va chercher le chemin exact vers le fichier spark-submit de pip
    spark_submit_path = os.path.join(os.environ["SPARK_HOME"], "bin", "spark-submit.cmd")

    spark_cmd = [
        spark_submit_path, # On utilise le chemin absolu généré
        "--packages", "io.graphframes:graphframes-spark4_2.13:0.11.0",
        "--driver-java-options", "-Dsun.security.jgss.native=true --add-opens=java.base/javax.security.auth=ALL-UNNAMED",
        "analyseur.py"
    ]

    # Sous Windows, spark-submit est parfois un fichier .cmd, shell=True aide à le trouver
    use_shell = sys.platform.startswith('win')

    print("\n--- Lancement de l'analyseur PySpark (spark-submit) ---")
    try:
        # Lance Spark et attend la fin de son exécution (bloquant)
        spark_proc = subprocess.run(spark_cmd, shell=use_shell)
    except KeyboardInterrupt:
        print("\nInterruption détectée par l'utilisateur.")
    finally:
        # 5. Nettoyage à la fermeture : on s'assure que le simulateur s'arrête
        print("\n--- Arrêt du simulateur ---")
        if sim_proc.poll() is None:  # Si le processus tourne encore
            if sys.platform.startswith('win'):
                sim_proc.terminate()
            else:
                os.kill(sim_proc.pid, signal.SIGTERM)
        print("Fin de l'exécution.")

if __name__ == "__main__":
    main()
