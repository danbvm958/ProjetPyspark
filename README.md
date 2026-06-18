# Projet

## creation virtual env

`python -m venv venv`

ou

`python3 -m venv venv`

## activer virtual env

`venv/Scripts/activate`

ou

`source venv/bin/activate`

ou 

`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process .\venv\Scripts\Activate.ps1`

## installation 

`pip install pyspark`

`pip install graphframes`


## lancement 

`python3 runpip.py`

`python runpip.py`

## interface graphique

`python interface.py`
L'interface se lance automatiquement également lorsque vous exécutez `python runpip.py`.

L'interface graphique affiche :
- le nombre d'événements reçus depuis `data/fluxDirect.json`
- les statistiques par actions, villes, catégories et vendeurs
- les métriques du graphe construit par Spark (sommets, arêtes, top nœuds)
- un historique des derniers événements reçus

Pour une interface moderne (recommandé) :

1. Installez les dépendances :

```bash
pip install -r requirements.txt
```

2. Lancez le projet normalement :

```bash
python runpip.py
```

Le script `runpip.py` démarre le simulateur, lance Spark puis ouvre automatiquement le dashboard Streamlit si `streamlit` est disponible dans l'environnement.

Vous pouvez aussi lancer directement l'interface :

```bash
streamlit run dashboard_app.py
```
