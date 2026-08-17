"""Tableau de bord Streamlit - Paris Basketball.

Réunit trois vues en une seule application, chacune dans son propre
onglet :
- "Revenu par match" : KPIs, revenu par match (billetterie + buvette),
  taux de remplissage, tableau détaillé.
- "Prédiction de revenu" : choix d'un adversaire déjà affronté, réglage
  d'un scénario (compétition/lieu/mois/jour), prédiction du revenu via
  le modèle entraîné par ml/train_model.py.
- "Copilote IA" : deux boutons ("Revenu par match" à gauche, "Prédiction
  de revenu" à droite) qui envoient les chiffres du premier onglet
  concerné à l'API Mistral et affichent le résumé en langage naturel.

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

Le Copilote IA nécessite une clé API dans .streamlit/secrets.toml (voir
secrets.toml.example) — en son absence, ses boutons affichent un message
explicite plutôt que de planter. Les onglets "Revenu par match" et
"Prédiction de revenu" s'exécutant à chaque run (Streamlit ne rend pas
les onglets à la demande), leurs derniers chiffres sont toujours
disponibles via st.session_state pour le Copilote, même si on ne les a
pas ouverts dans ce run-ci.

Utilisation :
    streamlit run app.py
"""
from pathlib import Path

import joblib
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from mistralai.client import Mistral

APP_DIR = Path(__file__).resolve().parent
WAREHOUSE_LIVE = APP_DIR.parent / "Data Warehouse" / "Rev Paris Basketball"
WAREHOUSE_SNAPSHOT = APP_DIR / "data"
WAREHOUSE = WAREHOUSE_LIVE if WAREHOUSE_LIVE.exists() else WAREHOUSE_SNAPSHOT

# Couleur "ballon de basket" — accent de la marque, réutilisée pour la
# série buvette dans les graphiques (cohérence entre les onglets).
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


# ---------------------------------------------------------------------------
# Copilote IA (API Mistral)
# ---------------------------------------------------------------------------

def get_mistral_client():
    """Renvoie un client Mistral si une clé API est configurée dans
    .streamlit/secrets.toml, sinon None (pas d'exception : les boutons
    Copilote IA gèrent ce cas en affichant un message). st.secrets lève
    une erreur (pas juste une absence de clé) si le fichier secrets.toml
    n'existe pas du tout, ce qui est le cas tant que la clé n'a pas été
    ajoutée — d'où le try/except plutôt qu'un simple .get()."""
    try:
        api_key = st.secrets.get("MISTRAL_API_KEY")
    except Exception:
        return None
    if not api_key:
        return None
    return Mistral(api_key=api_key)


def run_ai_summary(prompt):
    """Appelle l'API Mistral avec `prompt` et affiche le résumé (message
    clair si la clé API n'est pas configurée, ou en cas d'erreur réseau/API)."""
    client = get_mistral_client()
    if client is None:
        st.warning(
            "Clé API Mistral non configurée. Ajoute `MISTRAL_API_KEY` dans "
            "`.streamlit/secrets.toml` (voir `secrets.toml.example`) en local, "
            "ou dans les *Secrets* de l'appli sur Streamlit Cloud."
        )
        return
    with st.spinner("Génération du résumé..."):
        try:
            response = client.chat.complete(
                model="mistral-small-latest",
                messages=[{"role": "user", "content": prompt}],
            )
            st.info(response.choices[0].message.content)
        except Exception as e:
            st.error(f"Erreur lors de l'appel à l'API Mistral : {e}")


def build_revenue_prompt(ctx):
    date_range = ctx["date_range"]
    periode = (
        f"du {date_range[0].strftime('%d/%m/%Y')} au {date_range[1].strftime('%d/%m/%Y')}"
        if len(date_range) == 2 else "sur toute la saison"
    )
    return (
        "Tu es analyste pour le club de basketball Paris Basketball. Voici les "
        f"chiffres de revenu (billetterie + buvette) pour {ctx['n_matches']} match(s), "
        f"compétitions {', '.join(ctx['competitions'])}, {periode} :\n"
        f"- Revenu total : {ctx['total_revenue']:,.0f} €\n"
        f"- Revenu moyen par match : {ctx['avg_revenue']:,.0f} €\n"
        f"- Meilleur match : {ctx['best_match']} ({ctx['best_revenue']:,.0f} €)\n"
        f"- Moins bon match : {ctx['worst_match']} ({ctx['worst_revenue']:,.0f} €)\n"
        f"- Taux de remplissage moyen : {ctx['avg_fill_rate']:.0f}%\n\n"
        "Rédige un résumé court (4-5 phrases), en français, clair et concret, "
        "pour un dirigeant du club qui n'a pas le temps de lire le tableau détaillé."
    )


def build_prediction_prompt(p):
    return (
        "Tu es analyste pour le club de basketball Paris Basketball. Voici un "
        "scénario de match simulé et sa prédiction de revenu (billetterie + buvette) :\n"
        f"- Adversaire : {p['opponent']}\n"
        f"- Compétition : {p['competition']}\n"
        f"- Lieu : {p['venue']}\n"
        f"- Mois : {p['month']}\n"
        f"- Jour : {'week-end' if p['is_weekend'] else 'semaine'}\n"
        f"- Revenu prédit : {p['prediction']:,.0f} €\n"
        f"- Dernier revenu réel contre cet adversaire : {p['last_actual']:,.0f} €\n"
        f"- Marge d'erreur typique du modèle : ± {p['mae']:,.0f} € "
        f"(modèle entraîné sur seulement {p['n_matches']} matchs)\n\n"
        "Rédige un résumé court (3-4 phrases), en français, expliquant ce que "
        "cette prédiction signifie concrètement, avec la nuance nécessaire sur "
        "la fiabilité du modèle vu le peu de données d'entraînement."
    )


# ---------------------------------------------------------------------------
# Onglets
# ---------------------------------------------------------------------------

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
    worst_match = filtered.loc[filtered["revenu_total"].idxmin()]

    kpi_cols[0].metric("Revenu total", f"{total_revenue:,.0f} €".replace(",", " "))
    kpi_cols[1].metric("Matchs", f"{len(filtered)}")
    kpi_cols[2].metric("Revenu moyen / match", f"{filtered['revenu_total'].mean():,.0f} €".replace(",", " "))
    kpi_cols[3].metric(
        "Meilleur match",
        best_match["name"],
        f"{best_match['revenu_total']:,.0f} €".replace(",", " "),
    )

    # Dispo pour l'onglet Copilote IA, même s'il n'est pas ouvert ce run-ci.
    st.session_state["revenue_summary_context"] = {
        "n_matches": len(filtered),
        "competitions": selected_competitions,
        "date_range": date_range,
        "total_revenue": total_revenue,
        "avg_revenue": filtered["revenu_total"].mean(),
        "best_match": best_match["name"],
        "best_revenue": best_match["revenu_total"],
        "worst_match": worst_match["name"],
        "worst_revenue": worst_match["revenu_total"],
        "avg_fill_rate": filtered["taux_remplissage"].mean(),
    }

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

        # Dispo pour l'onglet Copilote IA, même après le rerun déclenché par
        # son propre bouton (qui n'est pas celui-ci).
        st.session_state["last_prediction"] = {
            "opponent": selected_opponent, "competition": competition, "venue": venue,
            "month": MONTH_NAMES_FR[month], "is_weekend": is_weekend,
            "prediction": prediction, "last_actual": last_match["revenu_total"],
            "mae": metrics["mae"], "n_matches": metrics["n_matches"],
        }


def render_copilot_tab():
    centered_title("Résumé")

    col_left, col_right = st.columns(2)

    with col_left:
        if st.button("Revenu par match", type="primary", width="stretch"):
            context = st.session_state.get("revenue_summary_context")
            if context is None:
                st.warning("Ouvre d'abord l'onglet « Revenu par match ».")
            else:
                run_ai_summary(build_revenue_prompt(context))

    with col_right:
        if st.button("Prédiction de revenu", type="primary", width="stretch"):
            prediction = st.session_state.get("last_prediction")
            if prediction is None:
                st.warning(
                    "Fais d'abord une prédiction dans l'onglet « Prédiction de revenu »."
                )
            else:
                run_ai_summary(build_prediction_prompt(prediction))


tab_revenue, tab_prediction, tab_copilot = st.tabs(
    ["📊 Revenu par match", "🔮 Prédiction de revenu", "🤖 Copilote IA"]
)
with tab_revenue:
    render_revenue_tab()
with tab_prediction:
    render_prediction_tab()
with tab_copilot:
    render_copilot_tab()
