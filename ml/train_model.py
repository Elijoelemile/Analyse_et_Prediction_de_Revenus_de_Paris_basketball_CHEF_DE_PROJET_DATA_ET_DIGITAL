"""Entraîne un modèle de prédiction du revenu par match.

Modèle : régression Ridge (régularisée, alpha choisi par validation
croisée interne via RidgeCV) sur les 41 matchs de la saison 2025-2026.

Features : compétition, lieu, mois, week-end (is_away exclu — constant
sur toute la saison observée, donc sans pouvoir prédictif). L'adversaire
n'est PAS une feature du modèle : avec 34 adversaires distincts pour 41
matchs, l'encoder directement ferait mémoriser le modèle plutôt que
généraliser. Il sert seulement, côté dashboard, à pré-remplir les autres
champs à partir de l'historique réel d'un adversaire choisi.

Cible : revenu_total (billetterie + buvette), calculé comme dans
dashboard/app.py (revenu boutique exclu, mêmes raisons que là-bas).

Vu le très faible nombre d'observations (41 matchs), le modèle est
volontairement simple et évalué par validation croisée leave-one-out
(LOO-CV) — on entraîne sur 40 matchs, on prédit le 41e, on répète pour
chacun — plutôt qu'un split train/test qui gaspillerait des données déjà
rares.

Écrit :
- dashboard-prediction/model.joblib — le pipeline entraîné (préprocesseur + modèle)
- dashboard-prediction/metrics.joblib — MAE/R² de la validation croisée
- dashboard-prediction/data/match_history.parquet — historique des 41 matchs
  (adversaire, compétition, mois, jour de semaine, lieu, revenu réel)

Utilisation :
    python train_model.py
"""
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import LeaveOneOut, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

PROJECT_DIR = Path(__file__).resolve().parent.parent
WAREHOUSE = PROJECT_DIR / "Data Warehouse" / "Rev Paris Basketball"
OUTPUT_DIR = PROJECT_DIR / "dashboard-prediction"

FEATURES_CATEGORICAL = ["competition_name", "venue_name"]
FEATURES_NUMERIC = ["month", "is_weekend"]


def build_match_history():
    """Reconstruit revenu_total par match (billetterie + buvette), comme
    dashboard/app.py, et ajoute les colonnes dérivées utilisées par le
    modèle (mois, jour de semaine, week-end, adversaire)."""
    billetterie = pd.read_parquet(WAREHOUSE / "fact_billetterie")
    buvette = pd.read_parquet(WAREHOUSE / "fact_buvette")
    matchs = pd.read_parquet(WAREHOUSE / "dim_matchs")

    rev_billetterie = billetterie.groupby("session_id")["amount"].sum().rename("revenu_billetterie")
    rev_buvette = buvette.groupby("session_id")["montant"].sum().rename("revenu_buvette")

    table = matchs.set_index("session_id").join([rev_billetterie, rev_buvette]).reset_index()
    table[["revenu_billetterie", "revenu_buvette"]] = (
        table[["revenu_billetterie", "revenu_buvette"]].fillna(0)
    )
    table["revenu_total"] = table["revenu_billetterie"] + table["revenu_buvette"]

    table["match_date"] = pd.to_datetime(table["match_date"])
    table["opponent"] = table["name"].str.split(" - ").str[-1]
    table["month"] = table["match_date"].dt.month
    table["day_of_week"] = table["match_date"].dt.day_name()
    table["is_weekend"] = table["match_date"].dt.dayofweek.isin([5, 6]).astype(int)

    return table.sort_values("match_date")


def build_pipeline():
    """Prétraitement (one-hot pour le catégoriel, passthrough pour le
    numérique) + RidgeCV, dont l'alpha est choisi par LOO interne —
    adapté à un très petit jeu de données."""
    preprocessor = ColumnTransformer([
        ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"), FEATURES_CATEGORICAL),
        ("num", "passthrough", FEATURES_NUMERIC),
    ])
    model = RidgeCV(alphas=np.logspace(-2, 3, 30))
    return Pipeline([("preprocess", preprocessor), ("model", model)])


def main():
    history = build_match_history()
    X = history[FEATURES_CATEGORICAL + FEATURES_NUMERIC]
    y = history["revenu_total"]

    pipeline = build_pipeline()

    # Évaluation honnête vu le faible N : leave-one-out cross-validation
    # (nichée : RidgeCV choisit aussi son alpha par LOO, sur les 40
    # restants à chaque itération — jamais sur l'observation testée).
    loo_predictions = cross_val_predict(pipeline, X, y, cv=LeaveOneOut())
    mae = mean_absolute_error(y, loo_predictions)
    r2 = r2_score(y, loo_predictions)

    print(f"Validation croisée leave-one-out sur {len(history)} matchs :")
    print(f"  MAE : {mae:,.0f} EUR (~{mae / y.mean() * 100:.0f}% du revenu moyen)")
    print(f"  R2  : {r2:.2f}")

    # Modèle final : entraîné sur la totalité des données (le LOO ci-dessus
    # ne sert qu'à estimer l'erreur, pas à produire le modèle livré).
    pipeline.fit(X, y)

    OUTPUT_DIR.mkdir(exist_ok=True)
    joblib.dump(pipeline, OUTPUT_DIR / "model.joblib")
    joblib.dump({"mae": mae, "r2": r2, "n_matches": len(history)}, OUTPUT_DIR / "metrics.joblib")

    data_dir = OUTPUT_DIR / "data"
    data_dir.mkdir(exist_ok=True)
    history[[
        "session_id", "name", "opponent", "match_date", "competition_name",
        "venue_name", "month", "day_of_week", "is_weekend", "revenu_total",
    ]].to_parquet(data_dir / "match_history.parquet", index=False)

    print(f"\nModèle -> {OUTPUT_DIR / 'model.joblib'}")
    print(f"Historique -> {data_dir / 'match_history.parquet'}")


if __name__ == "__main__":
    main()
