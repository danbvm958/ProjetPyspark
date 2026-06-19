import socket
import time
import random
import json
import os
from datetime import datetime

USERS = [
  { "user_id": "usr_9482", "user_city": "Paris" },
  { "user_id": "usr_1043", "user_city": "Lyon" },
  { "user_id": "usr_3321", "user_city": "Marseille" },
  { "user_id": "usr_5567", "user_city": "Lille" },
  { "user_id": "usr_8891", "user_city": "Bordeaux" },
  { "user_id": "usr_4412", "user_city": "Nantes" },
  { "user_id": "usr_2239", "user_city": "Toulouse" }
]

PRODUCTS = [
  { "product_id": "prod_5501", "product_cat": "Véhicules", "price": 4500.00, "seller_id": "sel_0214" },
  { "product_id": "prod_8832", "price": 12000.00, "seller_id": "sel_1150" },
  { "product_id": "prod_1120", "product_cat": "Immobilier", "price": 250000.00, "seller_id": "sel_4489" },
  { "product_id": "prod_4401", "product_cat": "Mode", "price": 45.00, "seller_id": "sel_7731" },
  { "product_id": "prod_9923", "product_cat": "Électronique", "price": 650.00, "seller_id": "sel_9022" },
  { "product_id": "prod_3312", "product_cat": "Mode", "price": 120.00, "seller_id": "sel_7731" },
  { "product_id": "prod_6674", "product_cat": "Électronique", "price": 35.00, "seller_id": "sel_0214" }
]

ACTIONS = ["AIME", "VOUT", "ACHAT"]


def simuler_action(users, products, actions):
    """
    Fonction pure : Prend des listes en entrée et retourne un tuple contenant 
    le produit choisi, l'action choisie, et le dictionnaire de l'événement généré.
    Aucun effet de bord.
    """
    user = random.choice(users)
    product = random.choice(products)
    action = random.choice(actions)
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')

    evenement = {
        "timestamp": timestamp,
        "user_id": user["user_id"],
        "user_city": user["user_city"],
        "product_id": product["product_id"],
        "product_cat": product.get("product_cat", "Inconnu"), 
        "seller_id": product["seller_id"],  
        "action_type": action,
        "price": product["price"]
    }
    
    return product, action, evenement


def retirer_produit(products, product_to_remove):
    """
    Fonction pure : Applique le principe d'immuabilité. 
    Au lieu de modifier la liste existante, elle en retourne une NOUVELLE 
    qui exclut le produit vendu.
    """
    return [p for p in products if p["product_id"] != product_to_remove["product_id"]]


def formater_evenement(evenement):
    """
    Fonction pure : Transforme un dictionnaire en chaîne JSON formatisée pour le flux.
    """
    return json.dumps(evenement) + "\n"


# --- ENTRÉES / SORTIES ET CYCLE DE VIE (Gestion des effets de bord) ---

def gerer_flux(conn, jsonfile):
    """
    Gère la boucle de simulation (l'état) et les effets de bord (Réseau / Fichier).
    """
    # On crée une copie de travail locale pour ne pas muter la constante globale PRODUCTS
    produits_disponibles = list(PRODUCTS)

    while produits_disponibles:
        product_choisi, action_choisie, evenement = simuler_action(USERS, produits_disponibles, ACTIONS)
        
        if action_choisie == "ACHAT":
            produits_disponibles = retirer_produit(produits_disponibles, product_choisi)
            print(f"[SÉCURITÉ] Produit {product_choisi['product_id']} vendu et retiré.")

        json_data = formater_evenement(evenement)

        try:
            conn.send(json_data.encode('utf-8'))
            jsonfile.write(json_data)
            print(f"[ENVOYÉ & ENREGISTRÉ] {json_data.strip()}")
            time.sleep(1)
        except Exception as e:
            print(f"[ERREUR] Connexion interrompue avec Spark : {e}")
            break
            
    if not produits_disponibles:
        print("[SIMULATEUR] Plus aucun produit disponible. Fin de la simulation.")


def main():
    os.makedirs("./data", exist_ok=True)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("localhost", 9990))
    s.listen(1)
    print("[SIMULATEUR] En attente que Spark Streaming se connecte sur le port 9990...")

    conn, addr = s.accept()
    print(f"[SIMULATEUR] Spark est connecté depuis {addr} ! Début de l'envoi...")

    with open("./data/fluxDirect.json", "a", encoding="utf-8", buffering=1) as jsonfile:
        gerer_flux(conn, jsonfile)

    conn.close()
    s.close()


if __name__ == "__main__":
    main()