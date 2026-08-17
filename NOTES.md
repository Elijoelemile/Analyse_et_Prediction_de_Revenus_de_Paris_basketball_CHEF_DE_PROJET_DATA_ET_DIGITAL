# Notes — Analyse et prédiction des revenus, Paris Basketball

## Ta question

Quel revenu chaque match de la saison 2025-2026 a-t-il généré (billetterie
+ buvette), quels facteurs l'expliquent, et peut-on estimer le revenu
attendu d'un match selon son adversaire, sa compétition, son lieu et sa
date ?

## Tes chiffres clés

- **41 matchs** analysés (saison 2025-2026, données réelles de billetterie
  et de buvette).
- **Revenu total : 21 094 901 €** (billetterie + buvette).
- **Revenu moyen par match : 514 510 €**, avec un écart considérable :
  de 154 128 € (PBB - Dijon) à 1 549 068 € (PBB - Olympiacos Piraeus).
- Modèle de prédiction (régression Ridge) : **R² = 0,78**, **erreur
  moyenne ≈ 109 301 € (~21 % du revenu moyen)**, validé par leave-one-out
  cross-validation.

## Ta réponse

Le revenu par match varie fortement (facteur ×10 entre le plus bas et le
plus haut), et ce n'est pas aléatoire : la **compétition** (Coupe
Européenne vs Championnat National vs Playoffs) et, dans une moindre
mesure, le **jour de la semaine** et le **lieu** expliquent l'essentiel
de cette variation — un modèle de régression simple avec seulement ces
quatre variables explique déjà 78 % de la variance observée.

Pour y arriver, un pipeline complet a été construit : extraction depuis
les fichiers sources bruts (JSON de billetterie, CSV de buvette et de
boutique), chargement dans un Data Lake, normalisation en Staging, puis
modélisation en schéma en étoile (tables de faits par processus métier —
billetterie, buvette, boutique, commandes, paiements — et dimensions
partagées matchs/client/date) dans un Data Warehouse. Le tout tourne sur
un cluster Spark conteneurisé, orchestré et planifié quotidiennement.

Deux tableaux de bord permettent d'exploiter ces résultats : un dashboard
Streamlit interactif (filtres, simulateur de prédiction, résumés par IA)
et un dashboard HTML autonome pour une consultation rapide sans rien
installer.

## Tes limites

- **Échantillon réduit (41 matchs)** : le modèle de prédiction est
  volontairement simple (régression Ridge, pas de gradient boosting ni
  deep learning) et évalué par validation croisée leave-one-out plutôt
  qu'un split train/test, pour ne pas gaspiller des données déjà rares.
  Une marge d'erreur de ~21 % doit être gardée à l'esprit pour toute
  décision basée sur une prédiction.
- **Le revenu boutique n'est pas inclus** dans l'analyse "revenu par
  match" : les ventes en magasin n'ont aucune référence fiable à un match
  précis dans les données sources (seule une minorité de clients
  n'ayant vu qu'un seul match dans la saison permettent une attribution
  non ambiguë — le reste est exclu plutôt que d'être mal attribué).
- **Le calendrier de la saison prochaine n'est pas disponible** dans les
  données actuelles : le simulateur de prédiction fonctionne sur des
  scénarios (adversaire déjà rencontré + paramètres choisis), pas sur de
  vrais matchs futurs. Il faudra réentraîner/réappliquer le modèle une
  fois ce calendrier connu.
- **Qualité des données sources** : quelques commandes ont été réexportées
  en double d'un fichier à l'autre (dédoublonnées dans le pipeline), et le
  nom d'un même adversaire varie parfois d'un match à l'autre (ex : "Monaco"
  vs "AS Monaco") — non corrigé automatiquement pour éviter d'imposer une
  règle de normalisation arbitraire.
- **`is_cancelled` a un sens contre-intuitif sur les paiements** : `True`
  correspond à des tentatives de paiement échouées/dupliquées, pas à de
  vraies annulations — vérifié ligne à ligne sur les données avant de
  filtrer, plutôt que supposé.
- Le modèle donne une **tendance directionnelle utile pour comparer des
  scénarios entre eux**, pas un chiffre garanti pour un budget.
