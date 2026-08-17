# Analyse et prédiction des revenus — Paris Basketball

Pipeline ELT complet (Extract → Load → Stage → Transform) sur Spark/Docker,
modélisation en étoile, prédiction de revenu par match, et deux tableaux de
bord (un Streamlit interactif, un HTML autonome).

## Structure du projet

```
elt/                  Scripts du pipeline (extract, load, stage, transform)
config/                Configuration partagée (logging, sources)
vm cluster/             Cluster Spark (Docker Compose + image)
orchestration/          Orchestrateur (enchaîne les 3 phases, planifié quotidiennement)
ml/                     Entraînement du modèle de prédiction
dashboard/              Tableau de bord Streamlit (revenu, prédiction, copilote IA)
gold/                   Tables agrégées, exportées en CSV (consultables sans rien lancer)
dashboard.html          Tableau de bord HTML autonome (aucun serveur requis)
data modeling/           Documentation de la modélisation (schéma en étoile, PDF)
NOTES.md                Question, chiffres clés, réponse, limites
```

Les dossiers `Data Lake/`, `Staging/`, `Data Warehouse/`, `logs/`,
`log storage/` sont générés par le pipeline et volontairement absents du
dépôt (voir `.gitignore`) — `gold/` et `dashboard/data/` en sont des
exports figés, suffisants pour explorer les résultats sans relancer le
pipeline.

## Installation

Prérequis : Python 3.12+, [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```bash
# 1. Environnement virtuel
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt

# 2. Cluster Spark (uniquement nécessaire pour relancer le pipeline complet)
cd "vm cluster"
docker compose build
docker compose up -d --scale spark-worker=2
cd ..
```

## Lancer le pipeline complet (optionnel — les résultats sont déjà dans `gold/`)

Le pipeline part d'une archive source déclarée dans `config/sources_config.py`
(chemin local, propre à la machine qui l'a exécuté). Une fois le cluster
démarré :

```bash
cd orchestration
python run_pipeline.py
```

Enchaîne automatiquement `load.py` → `stage.py` → `transform.py` sur le
cluster, avec gestion de dépendance (une étape ne démarre que si la
précédente a réussi), historique des exécutions (`logs/run_history.jsonl`)
et archivage quotidien des logs (`log storage/`). Une tâche planifiée
Windows peut l'exécuter tous les jours (voir
`orchestration/register_scheduled_task.ps1`).

Pour ré-entraîner le modèle de prédiction après un nouveau run du pipeline :

```bash
cd ml
python train_model.py
```

## Lancer le tableau de bord Streamlit

```bash
streamlit run dashboard/app.py
```

Trois onglets : revenu par match (KPIs, graphique, tableau détaillé),
prédiction de revenu (simulateur de scénario), et un copilote IA (résumés
en langage naturel via l'API Mistral — nécessite une clé dans
`dashboard/.streamlit/secrets.toml`, voir `secrets.toml.example` ;
fonctionne sans, avec un message explicite à la place des résumés).

## Consulter `dashboard.html`

Fichier autonome, sans serveur ni dépendance — double-clique dessus ou
ouvre-le dans un navigateur. Données figées au moment de sa génération
(mêmes chiffres que `gold/`).

## Vérifier un chiffre sans rien lancer

Les tables du dossier `gold/` (format CSV) contiennent les données déjà
agrégées : revenu par match, détail billetterie/buvette, calendrier des
matchs. Ouvrables directement dans Excel ou tout tableur.

## Documentation complémentaire

- `NOTES.md` — la question posée, les chiffres clés, la réponse, les limites.
- `data modeling/Modelisation_star_schema.pdf` — détail du schéma en étoile
  et des décisions de modélisation.
