import socket
import time
import random
import json
import os
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

products = [
  { "product_id": "prod_5501", "product_cat": "Véhicules", "price": 4500.00, "seller_id": "sel_0214" },
  { "product_id": "prod_8832", "price": 12000.00, "seller_id": "sel_1150" },
  { "product_id": "prod_1120", "product_cat": "Immobilier", "price": 250000.00, "seller_id": "sel_4489" },
  { "product_id": "prod_4401", "product_cat": "Mode", "price": 45.00, "seller_id": "sel_7731" },
  { "product_id": "prod_9923", "product_cat": "Électronique", "price": 650.00, "seller_id": "sel_9022" },
  { "product_id": "prod_3312", "product_cat": "Mode", "price": 120.00, "seller_id": "sel_7731" },
  { "product_id": "prod_6674", "product_cat": "Électronique", "price": 35.00, "seller_id": "sel_0214" }
]

actions = ["AIME", "VOUT", "ACHAT"]

def main():
    # Assurer que le dossier data existe
    os.makedirs("./data", exist_ok=True)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("localhost", 9990))
    s.listen(1)
    print("[SIMULATEUR] En attente que Spark Streaming se connecte sur le port 9990...")

    conn, addr = s.accept()
    print(f"[SIMULATEUR] Spark est connecté depuis {addr} ! Début de l'envoi...")

    with open("./data/fluxDirect.json", "a", encoding="utf-8", buffering=1) as jsonfile:
        while True:
            if not products:
                print("[SIMULATEUR] Plus aucun produit disponible. Fin de la simulation.")
                break

            user_choisi = random.choice(users)
            product_choisi = random.choice(products)
            action_choisie = random.choice(actions)
            
            timestamp_actuel = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')

            evenement = {
                "timestamp": timestamp_actuel,
                "user_id": user_choisi["user_id"],
                "user_city": user_choisi["user_city"],
                "product_id": product_choisi["product_id"],
                "product_cat": product_choisi.get("product_cat", "Inconnu"), 
                "seller_id": product_choisi["seller_id"],  
                "action_type": action_choisie,
                "price": product_choisi["price"]
            }

            if action_choisie == "ACHAT":
                products.remove(product_choisi)
                print(f"[SÉCURITÉ] Produit {product_choisi['product_id']} vendu et retiré.")

            try:
                json_data = json.dumps(evenement) + "\n"
                
                # 1. On envoie en direct à Spark via le socket
                conn.send(json_data.encode('utf-8'))
                
                # 2. On écrit en même temps dans le fichier local pour backup
                jsonfile.write(json_data)
                
                print(f"[ENVOYÉ & ENREGISTRÉ] {json_data.strip()}")
                time.sleep(1)
                
            except Exception as e:
                print(f"[ERREUR] Connexion interrompue avec Spark : {e}")
                break

    conn.close()
    s.close()

if __name__ == "__main__":
    main()