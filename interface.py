#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dashboard du projet PySpark / GraphFrames.
Cette interface graphique lit les données du simulateur et du dashboard Spark,
ets affiche un résumé clair et visuel des métriques métiers.
"""

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

BASE_PATH = Path(__file__).resolve().parent
DATA_ROOT = BASE_PATH / "data"
FLUX_PATH = DATA_ROOT / "fluxDirect.json"
VERTICES_PATH = DATA_ROOT / "dashboard" / "vertices"
EDGES_PATH = DATA_ROOT / "dashboard" / "edges"


def read_json_lines(path):
    if not path.exists():
        return []
    rows = []
    try:
        with path.open("r", encoding="utf-8") as fp:
            for raw in fp:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rows.append(json.loads(raw))
                except json.JSONDecodeError:
                    continue
    except Exception:
        return []
    return rows


def read_dashboard_records(folder):
    if not folder.exists():
        return []
    records = []
    for file_path in sorted(folder.glob("*.json")):
        if file_path.name.startswith("_SUCCESS"):
            continue
        records.extend(read_json_lines(file_path))
    return records


def build_metrics(events, vertices, edges):
    metrics = {}
    metrics["events"] = events
    metrics["vertices"] = vertices
    metrics["edges"] = edges

    metrics["event_count"] = len(events)
    metrics["user_count"] = len({e.get("user_id") for e in events if e.get("user_id")})
    metrics["product_count"] = len({e.get("product_id") for e in events if e.get("product_id")})
    metrics["seller_count"] = len({e.get("seller_id") for e in events if e.get("seller_id")})
    metrics["vertex_count"] = len(vertices)
    metrics["edge_count"] = len(edges)

    metrics["actions"] = Counter(e.get("action_type", "Inconnu") for e in events)
    metrics["cities"] = Counter(e.get("user_city", "Inconnue") for e in events)
    metrics["categories"] = Counter(e.get("product_cat", "Inconnue") for e in events)
    metrics["sellers"] = Counter(e.get("seller_id", "Inconnu") for e in events)
    metrics["node_types"] = Counter(v.get("type", "?") for v in vertices)
    metrics["relationships"] = Counter(e.get("relationship", "?") for e in edges)

    node_degree = Counter()
    for edge in edges:
        if edge.get("src"):
            node_degree[edge["src"]] += 1
        if edge.get("dst"):
            node_degree[edge["dst"]] += 1
    metrics["top_nodes"] = node_degree.most_common(8)

    metrics["top_actions"] = metrics["actions"].most_common(6)
    metrics["top_cities"] = metrics["cities"].most_common(6)
    metrics["top_categories"] = metrics["categories"].most_common(6)
    metrics["top_sellers"] = metrics["sellers"].most_common(6)

    metrics["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return metrics


def draw_bars(canvas, items, width, height, title):
    canvas.delete("all")
    if not items:
        canvas.create_text(width // 2, height // 2, text="Aucune donnée", fill="#999999", font=("Segoe UI", 10))
        return

    max_value = max(value for _, value in items) or 1
    margin = 10
    bar_height = max((height - margin * 2) // len(items) - 10, 20)
    bar_width = width - 150
    y = margin
    canvas.create_text(margin, margin, anchor="nw", text=title, font=("Segoe UI", 10, "bold"), fill="#222222")

    for label, value in items:
        ratio = value / max_value
        current_width = int(bar_width * ratio)
        canvas.create_rectangle(margin, y + 18, margin + current_width, y + 18 + bar_height, fill="#2d89ef", outline="")
        canvas.create_text(margin, y + 18 + bar_height / 2, anchor="w", text=f"{label}", fill="#111111", font=("Segoe UI", 9))
        canvas.create_text(width - 40, y + 18 + bar_height / 2, anchor="e", text=str(value), fill="#111111", font=("Segoe UI", 9, "bold"))
        y += bar_height + 14


class Dashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Dashboard LeBonCoin Streaming Graph")
        self.geometry("1040x780")
        self.minsize(980, 700)
        self.configure(background="#f4f5f8")
        self.create_style()
        self.build_layout()
        self.load_data()

    def create_style(self):
        self.style = ttk.Style(self)
        self.style.theme_use("clam")
        self.style.configure("TLabel", background="#f4f5f8", font=("Segoe UI", 10), foreground="#222222")
        self.style.configure("Header.TLabel", font=("Segoe UI", 14, "bold"), foreground="#1f4e79")
        self.style.configure("Title.TLabel", font=("Segoe UI", 11, "bold"), foreground="#333333")
        self.style.configure("TButton", font=("Segoe UI", 10), padding=6)
        self.style.configure("Card.TFrame", background="#ffffff", relief="flat")
        self.style.configure("CardHeader.TLabel", background="#ffffff", font=("Segoe UI", 11, "bold"), foreground="#1f4e79")

    def build_layout(self):
        header = ttk.Label(self, text="Tableau de bord - Streaming d'interactions commerciales", style="Header.TLabel")
        header.pack(padx=18, pady=(18, 8), anchor="w")

        controls = ttk.Frame(self, style="Card.TFrame")
        controls.pack(fill="x", padx=18, pady=(0, 12))
        controls.columnconfigure(1, weight=1)

        self.refresh_button = ttk.Button(controls, text="Actualiser", command=self.load_data)
        self.refresh_button.grid(row=0, column=0, padx=(0, 6), pady=8, sticky="w")

        self.status_label = ttk.Label(controls, text="Chargement...", style="TLabel")
        self.status_label.grid(row=0, column=1, sticky="w")

        tab_control = ttk.Notebook(self)
        tab_control.pack(fill="both", expand=True, padx=18, pady=(0, 18))

        self.overview_tab = ttk.Frame(tab_control)
        self.graph_tab = ttk.Frame(tab_control)
        self.log_tab = ttk.Frame(tab_control)

        tab_control.add(self.overview_tab, text="Vue générale")
        tab_control.add(self.graph_tab, text="Graph & métriques")
        tab_control.add(self.log_tab, text="Flux / historique")

        self.build_overview_tab()
        self.build_graph_tab()
        self.build_log_tab()

    def build_overview_tab(self):
        card = ttk.Frame(self.overview_tab, style="Card.TFrame")
        card.pack(fill="both", expand=True, padx=12, pady=12)
        card.columnconfigure((0, 1, 2), weight=1)

        stats = [
            ("Nombre d'événements", "event_count"),
            ("Utilisateurs uniques", "user_count"),
            ("Produits uniques", "product_count"),
            ("Vendeurs uniques", "seller_count"),
            ("Vertices totaux", "vertex_count"),
            ("Edges totales", "edge_count"),
        ]

        self.stat_labels = {}
        for index, (title, key) in enumerate(stats):
            frame = ttk.Frame(card, style="Card.TFrame", padding=12)
            frame.grid(row=index // 3, column=index % 3, sticky="nsew", padx=8, pady=8)
            frame.columnconfigure(0, weight=1)
            title_label = ttk.Label(frame, text=title, style="Title.TLabel")
            title_label.pack(anchor="w")
            value_label = ttk.Label(frame, text="-", font=("Segoe UI", 18, "bold"), foreground="#1f4e79")
            value_label.pack(anchor="w", pady=(8, 0))
            self.stat_labels[key] = value_label

        charts = ttk.Frame(self.overview_tab)
        charts.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        charts.columnconfigure((0, 1, 2), weight=1)

        self.action_canvas = tk.Canvas(charts, height=260, background="#ffffff", highlightthickness=0)
        self.action_canvas.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        self.city_canvas = tk.Canvas(charts, height=260, background="#ffffff", highlightthickness=0)
        self.city_canvas.grid(row=0, column=1, sticky="nsew", padx=6, pady=6)
        self.category_canvas = tk.Canvas(charts, height=260, background="#ffffff", highlightthickness=0)
        self.category_canvas.grid(row=0, column=2, sticky="nsew", padx=6, pady=6)

    def build_graph_tab(self):
        left = ttk.Frame(self.graph_tab, style="Card.TFrame")
        left.pack(side="left", fill="both", expand=True, padx=(12, 6), pady=12)
        right = ttk.Frame(self.graph_tab, style="Card.TFrame")
        right.pack(side="right", fill="both", expand=True, padx=(6, 12), pady=12)

        self.type_tree = self.build_tree(left, "Types de sommets")
        self.relationship_tree = self.build_tree(left, "Relations")
        self.top_nodes_tree = self.build_tree(right, "Top nœuds")
        self.top_sellers_tree = self.build_tree(right, "Top vendeurs")

    def build_log_tab(self):
        frame = ttk.Frame(self.log_tab, style="Card.TFrame")
        frame.pack(fill="both", expand=True, padx=12, pady=12)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        self.log_text = ScrolledText(frame, wrap="word", font=("Consolas", 10), background="#ffffff", state="disabled")
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)

    def build_tree(self, parent, title):
        frame = ttk.LabelFrame(parent, text=title, padding=12)
        frame.pack(fill="both", expand=True, padx=6, pady=6)
        tree = ttk.Treeview(frame, columns=("label", "value"), show="headings", selectmode="none")
        tree.heading("label", text="Valeur")
        tree.heading("value", text="Count")
        tree.column("label", anchor="w", width=160)
        tree.column("value", anchor="center", width=80)
        tree.pack(fill="both", expand=True)
        return tree

    def load_data(self):
        events = read_json_lines(FLUX_PATH)
        vertices = read_dashboard_records(VERTICES_PATH)
        edges = read_dashboard_records(EDGES_PATH)
        self.metrics = build_metrics(events, vertices, edges)
        self.update_interface()

    def update_interface(self):
        values = {
            "event_count": self.metrics["event_count"],
            "user_count": self.metrics["user_count"],
            "product_count": self.metrics["product_count"],
            "seller_count": self.metrics["seller_count"],
            "vertex_count": self.metrics["vertex_count"],
            "edge_count": self.metrics["edge_count"],
        }
        for key, widget in self.stat_labels.items():
            widget.config(text=str(values.get(key, 0)))

        self.status_label.config(text=f"Dernière actualisation : {self.metrics['updated_at']}")
        draw_bars(self.action_canvas, self.metrics["top_actions"], self.action_canvas.winfo_width() or 320, 260, "Actions")
        draw_bars(self.city_canvas, self.metrics["top_cities"], self.city_canvas.winfo_width() or 320, 260, "Villes")
        draw_bars(self.category_canvas, self.metrics["top_categories"], self.category_canvas.winfo_width() or 320, 260, "Catégories")

        self.populate_tree(self.type_tree, self.metrics["node_types"].items())
        self.populate_tree(self.relationship_tree, self.metrics["relationships"].items())
        self.populate_tree(self.top_nodes_tree, self.metrics["top_nodes"], is_pair=True)
        self.populate_tree(self.top_sellers_tree, self.metrics["top_sellers"], is_pair=True)
        self.update_log_text(self.metrics["events"])

    def populate_tree(self, tree, items, is_pair=False):
        for item in tree.get_children():
            tree.delete(item)
        rows = list(items)
        if not rows:
            tree.insert("", "end", values=("Aucune donnée", ""))
            return
        for item in rows:
            if is_pair:
                tree.insert("", "end", values=(item[0], item[1]))
            else:
                tree.insert("", "end", values=(item[0], item[1]))

    def update_log_text(self, events):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        if not events:
            self.log_text.insert("end", "Aucun événement disponible dans data/fluxDirect.json.\n")
        else:
            lines = []
            for event in events[-40:]:
                lines.append(
                    f"{event.get('timestamp', '?')}  |  {event.get('user_id', '?')}  |  {event.get('user_city', '?')}  |  {event.get('product_id', '?')}  |  {event.get('action_type', '?')}"
                )
            self.log_text.insert("end", "Derniers événements :\n\n")
            self.log_text.insert("end", "\n".join(lines))
        self.log_text.config(state="disabled")


def main():
    app = Dashboard()
    app.mainloop()


if __name__ == "__main__":
    main()
