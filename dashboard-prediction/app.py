"""Tableau de bord Streamlit - Prédiction de revenu par match.

Second dashboard, indépendant du premier (dashboard/app.py, qui reste
inchangé). Permet de choisir un adversaire parmi ceux déjà présents dans
l'historique (mêmes 41 matchs affichés dans le premier dashboard), puis
de simuler un match (compétition, mois, lieu, week-end) et d'en prédire
le revenu — pas forcément un match de la saison prochaine : un scénario
hypothétique quelconque, avec les paramètres de ton choix.

Charge le modèle entraîné par ml/train_model.py (dashboard-prediction/
model.joblib) et l'historique des matchs (dashboard-prediction/data/
match_history.parquet) — aucune dépendance à Spark, au cluster Docker,
ni à scikit-learn à l'entraînement : seule la prédiction (déjà légère)
tourne ici.

Utilisation :
    streamlit run app.py
"""
from pathlib import Path

import joblib
import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parent

MONTH_NAMES_FR = {
    1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril", 5: "Mai", 6: "Juin",
    7: "Juillet", 8: "Août", 9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre",
}

st.set_page_config(page_title="Prédiction de revenu par match — Paris Basketball", layout="wide")


@st.cache_resource
def load_model():
    return joblib.load(APP_DIR / "model.joblib"), joblib.load(APP_DIR / "metrics.joblib")


@st.cache_data
def load_history():
    return pd.read_parquet(APP_DIR / "data" / "match_history.parquet")


model, metrics = load_model()
history = load_history()

st.title("Prédiction de revenu par match")
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
st.subheader("1. Choisir un adversaire")
opponents = sorted(history["opponent"].unique())
selected_opponent = st.selectbox("Adversaire (déjà affronté cette saison)", opponents)

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
    )
with param_cols[1]:
    venue = st.selectbox(
        "Lieu", venues,
        index=venues.index(last_match["venue_name"]),
    )
with param_cols[2]:
    month = st.selectbox(
        "Mois", list(MONTH_NAMES_FR.keys()),
        index=int(last_match["month"]) - 1,
        format_func=lambda m: MONTH_NAMES_FR[m],
    )
with param_cols[3]:
    is_weekend = st.selectbox(
        "Jour", ["Semaine", "Week-end"],
        index=int(last_match["is_weekend"]),
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
