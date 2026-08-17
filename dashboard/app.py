"""Tableau de bord Streamlit - Paris Basketball.

Réunit deux vues en une seule application, chacune dans son propre
onglet :
- "Revenu par match" : KPIs, revenu par match (billetterie + buvette),
  taux de remplissage, tableau détaillé.
- "Prédiction de revenu" : choix d'un adversaire déjà affronté, réglage
  d'un scénario (compétition/lieu/mois/jour), prédiction du revenu via
  le modèle entraîné par ml/train_model.py.

Lit les tables Parquet (fact_billetterie, fact_buvette, dim_matchs,
match_history) et le modèle (model.joblib) — aucune dépendance à Spark,
au cluster Docker ni à scikit-learn à l'entraînement une fois ici :
seule la prédiction (déjà légère) tourne dans l'appli.

Deux sources de données possibles, choisies automatiquement :
- Data Warehouse/Rev Paris Basketball/ (à la racine du projet) si présent
  — le pipeline complet tourne sur cette machine (usage local).
- dashboard/data/ (embarqué dans le dépôt) sinon — utilisé sur Streamlit
  Community Cloud, qui n'a pas accès au cluster Spark ni à Data Warehouse/
  (ignoré par .gitignore). C'est un instantané figé : pour le rafraîchir,
  relancer ml/train_model.py puis recopier ses sorties dans dashboard/data/,
  puis commit + push.

Le revenu boutique n'est volontairement pas inclus (ni dans le premier
onglet, ni comme feature du modèle) : boutique_ventes_avoirs n'a pas de
référence directe à un match dans les données sources.

Utilisation :
    streamlit run app.py
"""
from pathlib import Path

import joblib
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

APP_DIR = Path(__file__).resolve().parent
WAREHOUSE_LIVE = APP_DIR.parent / "Data Warehouse" / "Rev Paris Basketball"
WAREHOUSE_SNAPSHOT = APP_DIR / "data"
WAREHOUSE = WAREHOUSE_LIVE if WAREHOUSE_LIVE.exists() else WAREHOUSE_SNAPSHOT

# Couleur "ballon de basket" — accent de la marque, réutilisée pour la
# série buvette dans les graphiques (cohérence entre les deux onglets).
BASKETBALL_ORANGE = "#eb6834"
COLOR_BILLETTERIE = "#2a78d6"
COLOR_BUVETTE = BASKETBALL_ORANGE
TEXT_SECONDARY = "#52514e"
GRIDLINE = "#e1e0d9"

MONTH_NAMES_FR = {
    1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril", 5: "Mai", 6: "Juin",
    7: "Juillet", 8: "Août", 9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre",
}

st.set_page_config(page_title="Paris Basketball — Revenus", layout="wide", page_icon="🏀")


def centered_title(text):
    """Titre de section centré, en orange 'ballon de basket'."""
    st.markdown(
        f"<h1 style='text-align:center; color:{BASKETBALL_ORANGE}; margin-bottom:0;'>{text}</h1>"
        f"<hr style='border:none; border-top:3px solid {BASKETBALL_ORANGE}; "
        f"width:80px; margin:8px auto 24px auto;'>",
        unsafe_allow_html=True,
    )


def _table_path(name):
    """Le Data Warehouse local stocke chaque table comme un dossier
    partitionné Spark (ex: fact_billetterie/) ; l'instantané embarqué est
    un unique fichier .parquet (ex: fact_billetterie.parquet). On accepte
    les deux, selon la source active."""
    folder = WAREHOUSE / name
    return folder if folder.exists() else WAREHOUSE / f"{name}.parquet"


@st.cache_data
def load_revenue_data():
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


@st.cache_resource
def load_model():
    # Le modèle et l'historique viennent toujours de dashboard/data/ (jamais
    # du Data Warehouse en direct) : ils sont produits à part par
    # ml/train_model.py, pas par le pipeline Spark.
    return joblib.load(WAREHOUSE_SNAPSHOT / "model.joblib"), joblib.load(WAREHOUSE_SNAPSHOT / "metrics.joblib")


@st.cache_data
def load_prediction_history():
    return pd.read_parquet(WAREHOUSE_SNAPSHOT / "match_history.parquet")


def render_revenue_tab():
    billetterie, buvette, matchs = load_revenue_data()
    match_table = build_match_table(billetterie, buvette, matchs)

    centered_title("Revenu par match")
    source_label = (
        "données en direct (Data Warehouse local)" if WAREHOUSE == WAREHOUSE_LIVE
        else "instantané embarqué (dashboard/data)"
    )
    st.caption(
        f"Billetterie + buvette, saison 2025-2026 — {source_label}. Le revenu boutique "
        "n'est pas attribuable à un match précis (voir la note en bas de page) et n'est pas inclus ici."
    )

    # ---------- Filtres (une seule ligne, au-dessus de tout) ----------
    filter_cols = st.columns([2, 2, 4])
    with filter_cols[0]:
        competitions = sorted(match_table["competition_name"].unique())
        selected_competitions = st.multiselect(
            "Compétition", competitions, default=competitions, key="rev_competitions",
        )
    with filter_cols[1]:
        min_date, max_date = match_table["match_date"].min(), match_table["match_date"].max()
        date_range = st.date_input(
            "Période", value=(min_date, max_date), min_value=min_date, max_value=max_date,
            key="rev_date_range",
        )

    filtered = match_table[match_table["competition_name"].isin(selected_competitions)]
    if len(date_range) == 2:
        start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
        filtered = filtered[(filtered["match_date"] >= start) & (filtered["match_date"] <= end)]

    if filtered.empty:
        st.warning("Aucun match ne correspond aux filtres sélectionnés.")
        return

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


def render_prediction_tab():
    model, metrics = load_model()
    history = load_prediction_history()

    centered_title("Prédiction de revenu")
    st.caption(
        "Choisis un adversaire déjà rencontré cette saison, ajuste les paramètres du "
        "match (ils ne sont pas obligés de correspondre à la saison prochaine — "
        "c'est un simulateur), et prédis le revenu billetterie + buvette attendu."
    )

    with st.expander("Fiabilité du modèle", expanded=False):
        st.write(
            f"Modèle de régression Ridge, entraîné et validé par validation croisée "
            f"leave-one-out sur les **{metrics['n_matches']} matchs** de la saison "
            f"2025-2026 (trop peu de matchs pour un modèle plus complexe ou un vrai "
            f"jeu de test séparé)."
        )
        col1, col2 = st.columns(2)
        col1.metric("Erreur moyenne (MAE)", f"{metrics['mae']:,.0f} €".replace(",", " "))
        col2.metric("R² (variance expliquée)", f"{metrics['r2']:.2f}")
        st.caption(
            "À titre indicatif seulement : une prédiction peut s'écarter du revenu "
            "réel d'environ 20% en moyenne. Utile pour comparer des scénarios entre "
            "eux, pas comme un chiffre garanti."
        )

    # ---------- Sélection de l'adversaire ----------
    st.subheader("1. Choisir un adversaire pour Paris Basketball")
    opponents = sorted(history["opponent"].unique())
    selected_opponent = st.selectbox(
        "Choisis un adversaire pour Paris Basketball", opponents, key="pred_opponent",
    )

    opponent_matches = history[history["opponent"] == selected_opponent].sort_values("match_date")
    last_match = opponent_matches.iloc[-1]

    if len(opponent_matches) > 1:
        st.caption(f"{selected_opponent} a été affronté {len(opponent_matches)} fois cette saison :")
        st.dataframe(
            opponent_matches[["match_date", "competition_name", "venue_name", "revenu_total"]].rename(columns={
                "match_date": "Date", "competition_name": "Compétition",
                "venue_name": "Lieu", "revenu_total": "Revenu réel (€)",
            }),
            hide_index=True,
            width="stretch",
            column_config={
                "Date": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Revenu réel (€)": st.column_config.NumberColumn(format="%.0f €"),
            },
        )
    else:
        st.caption(
            f"Match du {last_match['match_date'].strftime('%d/%m/%Y')} — "
            f"revenu réel : {last_match['revenu_total']:,.0f} €".replace(",", " ")
        )

    # ---------- Paramètres du scénario (pré-remplis depuis l'historique, modifiables) ----------
    st.subheader("2. Ajuster le scénario")
    param_cols = st.columns(4)
    competitions = sorted(history["competition_name"].unique())
    venues = sorted(history["venue_name"].unique())

    with param_cols[0]:
        competition = st.selectbox(
            "Compétition", competitions,
            index=competitions.index(last_match["competition_name"]),
            key="pred_competition",
        )
    with param_cols[1]:
        venue = st.selectbox(
            "Lieu", venues,
            index=venues.index(last_match["venue_name"]),
            key="pred_venue",
        )
    with param_cols[2]:
        month = st.selectbox(
            "Mois", list(MONTH_NAMES_FR.keys()),
            index=int(last_match["month"]) - 1,
            format_func=lambda m: MONTH_NAMES_FR[m],
            key="pred_month",
        )
    with param_cols[3]:
        is_weekend = st.selectbox(
            "Jour", ["Semaine", "Week-end"],
            index=int(last_match["is_weekend"]),
            key="pred_weekend",
        ) == "Week-end"

    # ---------- Prédiction ----------
    st.subheader("3. Prédire")
    if st.button("Prédire le revenu", type="primary"):
        scenario = pd.DataFrame([{
            "competition_name": competition,
            "venue_name": venue,
            "month": month,
            "is_weekend": int(is_weekend),
        }])
        prediction = model.predict(scenario)[0]

        result_cols = st.columns(3)
        result_cols[0].metric("Revenu prédit", f"{prediction:,.0f} €".replace(",", " "))
        result_cols[1].metric(
            "Dernier revenu réel vs cet adversaire",
            f"{last_match['revenu_total']:,.0f} €".replace(",", " "),
        )
        delta = prediction - last_match["revenu_total"]
        result_cols[2].metric(
            "Écart vs ce dernier match réel",
            f"{delta:+,.0f} €".replace(",", " "),
        )
        st.caption(
            f"Marge d'erreur typique du modèle : ± {metrics['mae']:,.0f} €".replace(",", " ")
        )


tab_revenue, tab_prediction = st.tabs(["📊 Revenu par match", "🔮 Prédiction de revenu"])
with tab_revenue:
    render_revenue_tab()
with tab_prediction:
    render_prediction_tab()
