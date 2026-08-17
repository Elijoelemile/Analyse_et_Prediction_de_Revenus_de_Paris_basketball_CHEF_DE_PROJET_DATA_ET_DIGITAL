r"""Registre des sources de données pour la phase d'extraction.

Chaque entrée décrit une source de données. `name` est le "nom principal"
de la source — il sert à regrouper ses fichiers une fois extraits (voir
extract.py).

Important : depuis le passage sur le cluster Spark (voir
docker-compose.yml), extract.py et load.py s'exécutent DANS les
conteneurs, pas sur Windows directement. Les chemins ci-dessous doivent
donc être les chemins tels qu'ils apparaissent dans les conteneurs
(définis par les volumes du docker-compose), pas les chemins Windows.

Trois types de source sont supportés :

- "path": un ou plusieurs fichiers locaux, trouvés via un motif glob.
          N'importe quel format fonctionne (json, csv, xlsx, xml, pdf, ...)
          — les fichiers sont lus tels quels, octet par octet.
- "zip":  des fichiers situés à l'intérieur d'une archive .zip (sans
          l'extraire entièrement sur le disque). `internal_pattern` est un
          motif (style glob, avec des "/" comme séparateurs) appliqué aux
          chemins internes du zip. Les entrées qui finissent en ".gz" sont
          automatiquement décompressées ; le nom de fichier obtenu perd
          alors son extension ".gz".
- "api":  un point d'accès HTTP GET. Le corps brut de la réponse est
          enregistré tel quel (non parsé), comme fichier snapshot horodaté.
          L'extension du fichier est déduite du Content-Type de la réponse,
          ou de l'URL si le Content-Type est absent/non reconnu. Le jeton
          d'authentification (s'il y en a un) est lu depuis la variable
          d'environnement nommée dans `auth_env_var`, envoyé en Bearer token.

Exemples de format :

SOURCES = [
    {
        "name": "orders",
        "type": "path",
        "pattern": "/app/data_externe/ORDERS_*.json",   # chemin monté dans le conteneur
    },
    {
        "name": "orders",
        "type": "zip",
        "zip_path": "/data/archive.zip",
        "internal_pattern": "dossier/sous_dossier/orders/*.json.gz",
    },
    {
        "name": "standings",
        "type": "api",
        "url": "https://api.example.com/v1/standings",
        "auth_env_var": "STANDINGS_API_KEY",  # optionnel
        "params": {},                          # paramètres de requête optionnels
    },
]
"""

SOURCES = [
    {
        "name": "orders",
        "type": "zip",
        "zip_path": "/data/wetransfer.zip",
        "internal_pattern": "A_ENVOYER_AU_CANDIDAT/test_data_pbb/dataset/sftp/orders/*.json.gz",
    },
    {
        "name": "fb_transactions",
        "type": "zip",
        "zip_path": "/data/wetransfer.zip",
        "internal_pattern": "A_ENVOYER_AU_CANDIDAT/test_data_pbb/dataset/fb_transactions.csv",
    },
    {
        "name": "boutique_ventes_avoirs",
        "type": "zip",
        "zip_path": "/data/wetransfer.zip",
        "internal_pattern": "A_ENVOYER_AU_CANDIDAT/test_data_pbb/dataset/boutique_ventes_avoirs.csv",
    },
]
