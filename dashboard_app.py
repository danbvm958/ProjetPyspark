#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Streamlit dashboard pour le projet PySpark / GraphFrames.
Affiche KPI, graphiques interactifs et l'historique des événements.
"""

import json
from pathlib import Path
from collections import Counter
import pandas as pd
import streamlit as st
import plotly.express as px

BASE = Path(__file__).resolve().parent
FLUX = BASE / "data" / "fluxDirect.json"
VERTICES = BASE / "data" / "dashboard" / "vertices"
EDGES = BASE / "data" / "dashboard" / "edges"


def read_json_lines(path: Path):
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
    return rows


@st.cache_data
def load_data():
    events = read_json_lines(FLUX)
    vertices = []
    edges = []
    if VERTICES.exists():
        for p in sorted(VERTICES.glob("*.json")):
            if p.name.startswith("_SUCCESS"):
                continue
            vertices.extend(read_json_lines(p))
    if EDGES.exists():
        for p in sorted(EDGES.glob("*.json")):
            if p.name.startswith("_SUCCESS"):
                continue
            edges.extend(read_json_lines(p))

    df_events = pd.DataFrame(events)
    df_vertices = pd.DataFrame(vertices)
    df_edges = pd.DataFrame(edges)
    return df_events, df_vertices, df_edges


def make_kpis(df_events, df_vertices, df_edges):
    kpis = {}
    kpis["Événements"] = len(df_events)
    kpis["Utilisateurs uniques"] = int(df_events["user_id"].nunique()) if not df_events.empty else 0
    kpis["Produits uniques"] = int(df_events["product_id"].nunique()) if not df_events.empty else 0
    kpis["Vendeurs uniques"] = int(df_events["seller_id"].nunique()) if not df_events.empty else 0
    kpis["Sommets"] = len(df_vertices)
    kpis["Arêtes"] = len(df_edges)
    return kpis


def chart_top(df, column, top=8, title=None):
    if df is None or df.empty or column not in df.columns:
        st.info("Aucune donnée pour ce graphique")
        return
    counts = df[column].value_counts().nlargest(top).reset_index()
    counts.columns = [column, "count"]
    fig = px.bar(counts, x=column, y="count", title=title or f"Top {top} {column}")
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.set_page_config(page_title="Dashboard Streaming Graph", layout="wide")

    st.title("Dashboard - Streaming d'interactions commerciales")
    st.markdown("Interface moderne pour visualiser le flux et le graphe produit par Spark.")

    df_events, df_vertices, df_edges = load_data()

    kpis = make_kpis(df_events, df_vertices, df_edges)

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Événements", kpis["Événements"])
    col2.metric("Utilisateurs", kpis["Utilisateurs uniques"])
    col3.metric("Produits", kpis["Produits uniques"])
    col4.metric("Vendeurs", kpis["Vendeurs uniques"])
    col5.metric("Sommets", kpis["Sommets"])
    col6.metric("Arêtes", kpis["Arêtes"])

    st.divider()

    left, right = st.columns([2, 1])

    with left:
        st.subheader("Distribution des actions / catégories / villes")
        if not df_events.empty:
            top_actions = df_events["action_type"].value_counts().reset_index()
            top_actions.columns = ["action", "count"]
            fig_a = px.pie(top_actions, names="action", values="count", title="Répartition des actions")
            st.plotly_chart(fig_a, use_container_width=True)

            chart_top(df_events, "user_city", top=8, title="Top villes")
            chart_top(df_events, "product_cat", top=8, title="Top catégories")
        else:
            st.info("Aucun événement trouvé. Attendez que le simulateur envoie des données.")

    with right:
        st.subheader("Métriques graphe")
        st.write(f"Sommets : {kpis['Sommets']}")
        st.write(f"Arêtes : {kpis['Arêtes']}")
        if not df_edges.empty:
            rels = df_edges["relationship"].value_counts().reset_index()
            rels.columns = ["relationship", "count"]
            fig_r = px.bar(rels, x="relationship", y="count", title="Relations")
            st.plotly_chart(fig_r, use_container_width=True)

    st.divider()

    st.subheader("Derniers événements (30)")
    if df_events.empty:
        st.info("Aucun événement disponible.")
    else:
        st.dataframe(df_events.sort_values(by="timestamp", ascending=False).head(30))

    st.sidebar.header("Actions")
    if st.sidebar.button("Actualiser les données"):
        load_data.clear()
        st.experimental_rerun()


if __name__ == "__main__":
    main()
