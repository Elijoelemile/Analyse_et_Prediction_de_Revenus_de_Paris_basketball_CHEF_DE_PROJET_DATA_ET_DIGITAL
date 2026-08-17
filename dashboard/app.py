"""Tableau de bord Streamlit - Revenu par match.

Lit les tables Parquet (fact_billetterie, fact_buvette, dim_matchs) —
aucune dépendance à Spark ou au cluster Docker à l'exécution : le
dashboard consomme le résultat déjà matérialisé par le pipeline (voir
elt/transform.py).

Deux sources possibles, choisies automatiquement :
- Data Warehouse/Rev Paris Basketball/ (à la racine du projet) si présent
  — le pipeline complet tourne sur cette machine (usage local).
- dashboard/data/ (embarqué dans le dépôt) sinon — utilisé sur Streamlit
  Community Cloud, qui n'a pas accès au cluster Spark ni à Data Warehouse/
  (ignoré par .gitignore). C'est un instantané figé : pour le rafraîchir,
  recopier les 3 tables depuis Data Warehouse/ puis commit + push.

Le revenu boutique n'est volontairement pas inclus dans ce tableau de
bord : boutique_ventes_avoirs n'a pas de référence directe à un match
(voir fact_boutique, où session_id n'est renseigné que pour les clients
n'ayant vu qu'un seul match dans la saison) — l'inclure fausserait le
"revenu par match" pour la majorité des lignes.

Utilisation :
    streamlit run app.py
"""
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

APP_DIR = Path(__file__).resolve().parent
WAREHOUSE_LIVE = APP_DIR.parent / "Data Warehouse" / "Rev Paris Basketball"
WAREHOUSE_SNAPSHOT = APP_DIR / "data"
WAREHOUSE = WAREHOUSE_LIVE if WAREHOUSE_LIVE.exists() else WAREHOUSE_SNAPSHOT

# Palette catégorielle (slot 1 = bleu, slot 2 = orange) — ordre fixe, jamais permuté.
COLOR_BILLETTERIE = "#2a78d6"
COLOR_BUVETTE = "#eb6834"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
TEXT_MUTED = "#898781"
GRIDLINE = "#e1e0d9"

st.set_page_config(page_title="Revenu par match — Paris Basketball", layout="wide")


def _table_path(name):
    """Le Data Warehouse local stocke chaque table comme un dossier
    partitionné Spark (ex: fact_billetterie/) ; l'instantané embarqué est
    un unique fichier .parquet (ex: fact_billetterie.parquet). On accepte
    les deux, selon la source active."""
    folder = WAREHOUSE / name
    return folder if folder.exists() else WAREHOUSE / f"{name}.parquet"


@st.cache_data
def load_data():
    billetterie = pd.read_parquet(_table_path("fact_billetterie"))
    buvette = pd.read_parquet(_table_path("fact_buvette"))
    matchs = pd.read_parquet(_table_path("dim_matchs"))
    return billetterie, buvette, matchs


def build_match_table(billetterie, buvette, matchs):
    rev_billetterie = billetterie.groupby("session_id")["amount"].sum().rename("revenu_billetterie")
    nb_billets = billetterie.groupby("session_id")["ticket_id"].count().rename("nb_billets")
    rev_buvette = buvette.groupby("session_id")["montant"].sum().rename("revenu_buvette")

    table = (
        matchs.set_index("session_id")
        .join([rev_billetterie, nb_billets, rev_buvette])
        .reset_index()
    )
    table[["revenu_billetterie", "revenu_buvette", "nb_billets"]] = (
        table[["revenu_billetterie", "revenu_buvette", "nb_billets"]].fillna(0)
    )
    table["revenu_total"] = table["revenu_billetterie"] + table["revenu_buvette"]
    table["taux_remplissage"] = table["nb_billets"] / table["venue_capacity"] * 100
    table["match_date"] = pd.to_datetime(table["match_date"])
    return table.sort_values("match_date")


billetterie, buvette, matchs = load_data()
match_table = build_match_table(billetterie, buvette, matchs)

st.title("Revenu par match")
source_label = "données en direct (Data Warehouse local)" if WAREHOUSE == WAREHOUSE_LIVE else "instantané embarqué (dashboard/data)"
st.caption(
    f"Billetterie + buvette, saison 2025-2026 — {source_label}. Le revenu boutique "
    "n'est pas attribuable à un match précis (voir la note en bas de page) et n'est pas inclus ici."
)

# ---------- Filtres (une seule ligne, au-dessus de tout) ----------
filter_cols = st.columns([2, 2, 4])
with filter_cols[0]:
    competitions = sorted(match_table["competition_name"].unique())
    selected_competitions = st.multiselect("Compétition", competitions, default=competitions)
with filter_cols[1]:
    min_date, max_date = match_table["match_date"].min(), match_table["match_date"].max()
    date_range = st.date_input("Période", value=(min_date, max_date), min_value=min_date, max_value=max_date)

filtered = match_table[match_table["competition_name"].isin(selected_competitions)]
if len(date_range) == 2:
    start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
    filtered = filtered[(filtered["match_date"] >= start) & (filtered["match_date"] <= end)]

if filtered.empty:
    st.warning("Aucun match ne correspond aux filtres sélectionnés.")
    st.stop()

# ---------- KPI ----------
kpi_cols = st.columns(4)
total_revenue = filtered["revenu_total"].sum()
best_match = filtered.loc[filtered["revenu_total"].idxmax()]

kpi_cols[0].metric("Revenu total", f"{total_revenue:,.0f} €".replace(",", " "))
kpi_cols[1].metric("Matchs", f"{len(filtered)}")
kpi_cols[2].metric("Revenu moyen / match", f"{filtered['revenu_total'].mean():,.0f} €".replace(",", " "))
kpi_cols[3].metric(
    "Meilleur match",
    best_match["name"],
    f"{best_match['revenu_total']:,.0f} €".replace(",", " "),
)

# ---------- Graphique : revenu par match, billetterie + buvette empilés ----------
fig = go.Figure()
fig.add_trace(go.Bar(
    x=filtered["name"], y=filtered["revenu_billetterie"], name="Billetterie",
    marker_color=COLOR_BILLETTERIE,
    hovertemplate="<b>%{customdata}</b><br>Billetterie : %{y:,.0f} €<extra></extra>",
    customdata=filtered["name"],
))
fig.add_trace(go.Bar(
    x=filtered["name"], y=filtered["revenu_buvette"], name="Buvette",
    marker_color=COLOR_BUVETTE,
    hovertemplate="<b>%{customdata}</b><br>Buvette : %{y:,.0f} €<extra></extra>",
    customdata=filtered["name"],
))
fig.update_layout(
    barmode="stack",
    bargap=0.15,
    template="plotly_white",
    height=460,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    xaxis=dict(title=None, tickangle=-45, gridcolor=GRIDLINE, categoryorder="array",
               categoryarray=filtered["name"]),
    yaxis=dict(title="Revenu (€)", gridcolor=GRIDLINE, tickformat=",.0f"),
    font=dict(color=TEXT_SECONDARY),
    plot_bgcolor="#fcfcfb",
    paper_bgcolor="#fcfcfb",
    margin=dict(t=10, b=10),
)
st.plotly_chart(fig, width="stretch")

# ---------- Table détaillée ----------
st.subheader("Détail par match")
display_table = filtered[[
    "match_date", "name", "competition_name", "venue_name",
    "revenu_billetterie", "revenu_buvette", "revenu_total",
    "nb_billets", "taux_remplissage",
]].rename(columns={
    "match_date": "Date", "name": "Match", "competition_name": "Compétition",
    "venue_name": "Lieu", "revenu_billetterie": "Billetterie (€)",
    "revenu_buvette": "Buvette (€)", "revenu_total": "Total (€)",
    "nb_billets": "Billets vendus", "taux_remplissage": "Taux de remplissage",
})
st.dataframe(
    display_table,
    width="stretch",
    hide_index=True,
    column_config={
        "Date": st.column_config.DateColumn(format="DD/MM/YYYY"),
        "Billetterie (€)": st.column_config.NumberColumn(format="%.0f €"),
        "Buvette (€)": st.column_config.NumberColumn(format="%.0f €"),
        "Total (€)": st.column_config.NumberColumn(format="%.0f €"),
        "Taux de remplissage": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.0f%%"),
    },
)

st.caption(
    "Le revenu boutique (ventes en magasin) n'a pas de référence directe à un "
    "match dans les données sources — il n'est donc pas inclus dans ce tableau "
    "de bord pour ne pas fausser le revenu par match."
)
