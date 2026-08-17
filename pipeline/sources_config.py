r"""Registre des sources de données pour la phase d'extraction.

Chaque entrée décrit une source de données. `name` est le "nom principal"
de la source — il sert à regrouper ses fichiers une fois extraits en
mémoire (voir extract.py). Rien n'est écrit sur le disque à cette étape.

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
        "pattern": r"C:\chemin\vers\ORDERS_*.json",   # n'importe quelle extension fonctionne
    },
    {
        "name": "orders",
        "type": "zip",
        "zip_path": r"C:\chemin\vers\archive.zip",
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
        "zip_path": r"C:\Users\Эли Жоэль\wetransfer_a_envoyer_au_candidat_2026-07-29_0843.zip",
        "internal_pattern": "A_ENVOYER_AU_CANDIDAT/test_data_pbb/dataset/sftp/orders/*.json.gz",
    },
    {
        "name": "fb_transactions",
        "type": "zip",
        "zip_path": r"C:\Users\Эли Жоэль\wetransfer_a_envoyer_au_candidat_2026-07-29_0843.zip",
        "internal_pattern": "A_ENVOYER_AU_CANDIDAT/test_data_pbb/dataset/fb_transactions.csv",
    },
    {
        "name": "boutique_ventes_avoirs",
        "type": "zip",
        "zip_path": r"C:\Users\Эли Жоэль\wetransfer_a_envoyer_au_candidat_2026-07-29_0843.zip",
        "internal_pattern": "A_ENVOYER_AU_CANDIDAT/test_data_pbb/dataset/boutique_ventes_avoirs.csv",
    },
]
