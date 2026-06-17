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
    """Configure la variable JAVA_HOME de manière dynamique si nécessaire."""
    # Optionnel : Si vous voulez forcer un chemin sous Linux uniquement
    if not sys.platform.startswith('win'):
        linux_java = "/usr/lib/jvm/java-17-openjdk-amd64"
        if os.path.exists(linux_java):
            os.environ["JAVA_HOME"] = linux_java
            os.environ["PATH"] = f"{linux_java}/bin:{os.environ['PATH']}"
            print(f"Java 17 configuré pour Linux : {linux_java}")
    else:
        # Sous Windows, il s'appuiera sur le JAVA_HOME du système. 
        # Vous pouvez décommenter la ligne suivante si vous voulez forcer un chemin Windows :
        # os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
        print(f"Windows: Utilisation du JAVA_HOME système : {os.environ.get('JAVA_HOME', 'Non défini')}")

def main():
    # 1. Libérer le port 9990
    kill_process_on_port(PORT)
    time.sleep(1)

    # 2. Configurer l'environnement
    setup_java_env()

    # 3. Lancer le simulateur en arrière-plan
    print("\n--- Lancement du simulateur.py ---")
    sim_proc = subprocess.Popen([sys.executable, "simulateur.py"])
    
    # Pause pour laisser le socket se lier (bind)
    time.sleep(2)

    # 4. Préparer la commande Spark-Submit
    spark_cmd = [
        "spark-submit",
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
