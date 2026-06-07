import socket
import threading
import time
import random
import json
from datetime import datetime

users = [
  { "user_id": "usr_9482", "user_city": "Paris" },
  { "user_id": "usr_1043", "user_city": "Lyon" },
  { "user_id": "usr_3321", "user_city": "Marseille" },
  { "user_id": "usr_5567", "user_city": "Lille" },
  { "user_id": "usr_8891", "user_city": "Bordeaux" },
  { "user_id": "usr_4412", "user_city": "Nantes" },
  { "user_id": "usr_2239", "user_city": "Toulouse" }
]

sellers = [
  { "seller_id": "sel_0214" },
  { "seller_id": "sel_1150" },
  { "seller_id": "sel_4489" },
  { "seller_id": "sel_7731" },
  { "seller_id": "sel_9022" }
]

products = [
  { "product_id": "prod_5501", "product_cat": "Véhicules", "price": 4500.00, "seller_id": "sel_0214" },
  { "product_id": "prod_8832", "product_cat": "Véhicules", "price": 12000.00, "seller_id": "sel_1150" },
  { "product_id": "prod_1120", "product_cat": "Immobilier", "price": 250000.00, "seller_id": "sel_4489" },
  { "product_id": "prod_4401", "product_cat": "Mode", "price": 45.00, "seller_id": "sel_7731" },
  { "product_id": "prod_9923", "product_cat": "Électronique", "price": 650.00, "seller_id": "sel_9022" },
  { "product_id": "prod_3312", "product_cat": "Mode", "price": 120.00, "seller_id": "sel_7731" },
  { "product_id": "prod_6674", "product_cat": "Électronique", "price": 35.00, "seller_id": "sel_0214" }
]

actions = ["AIME", "VOUT", "ACHAT"]

#
# SERVEUR 
#
def serveur():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("localhost", 9990)) 
    s.listen(1)
    print("[SERVEUR] En attente de connexion...")

    conn, addr = s.accept()
    print(f"[SERVEUR] Client connecté depuis {addr}")

    while True:
        try:
            data = conn.recv(1024).decode().strip()
            if not data:
                break
            print(f"[SERVEUR REÇU] {data}")
        except Exception as e:
            print(f"[SERVEUR ERREUR] {e}")
            break
    conn.close()

#
# CLIENT 
#
def client():
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    while True:
        try:
            print("[CLIENT] Tentative de connexion au serveur...")
            c.connect(("localhost", 9990)) 
            print("[CLIENT] Connecté au serveur avec succès !")
            break 
        except ConnectionRefusedError:
            print("[CLIENT] Serveur indisponible, nouvelle tentative dans 1 seconde...")
            time.sleep(1)

    while True:
        user_choisi = random.choice(users)
        product_choisi = random.choice(products)
        action_choisie = random.choice(actions)
        
        timestamp_actuel = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        evenement = {
            "timestamp": timestamp_actuel,
            "user_id": user_choisi["user_id"],
            "user_city": user_choisi["user_city"],
            "product_id": product_choisi["product_id"],
            "product_cat": product_choisi["product_cat"],
            "seller_id": product_choisi["seller_id"],  
            "action_type": action_choisie,
            "price": product_choisi["price"]
        }

        try:
            # Sérialisation en JSON + saut de ligne + encodage en bytes
            json_data = json.dumps(evenement) + "\n"
            c.send(json_data.encode())
            time.sleep(1)
        except Exception as e:
            print(f"[CLIENT ERREUR] Perte de connexion : {e}")
            break

# Lancement des threads
t1 = threading.Thread(target=serveur, daemon=True)
t2 = threading.Thread(target=client, daemon=True)

t1.start()
t2.start()

while True:
    time.sleep(10)