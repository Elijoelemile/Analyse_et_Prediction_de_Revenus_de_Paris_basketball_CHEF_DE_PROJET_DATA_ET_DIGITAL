"""Pipeline ELT - Phase 1 : Extraction.

Récupère les données brutes de chaque source déclarée dans
sources_config.SOURCES et les garde en mémoire — rien n'est écrit sur le
disque. `extract_all()` est un générateur qui produit des triplets
(nom_source, nom_fichier, contenu_brut) ; c'est la fonction à importer
depuis la phase suivante (Load/Transform) pour consommer les données
directement.

Une erreur sur un fichier ou une source (fichier introuvable, zip
corrompu, requête API en échec...) est loggée et n'interrompt pas le
reste de l'extraction — voir logs/pipeline.log.

Utilisation en script (juste pour vérifier que l'extraction fonctionne,
affiche un résumé sans rien persister) :
    python extract.py
"""
import fnmatch
import gzip
import mimetypes
import os
import zipfile
from datetime import datetime
from glob import glob
from pathlib import Path
from urllib.parse import urlparse

import requests

from logging_config import get_logger
from sources_config import SOURCES

logger = get_logger("extract")


def extract_path_source(source):
    """Génère (nom_fichier, contenu) pour chaque fichier local correspondant au motif glob."""
    matches = glob(source["pattern"])
    if not matches:
        logger.warning(f"aucun fichier ne correspond au motif : {source['pattern']}")
        return

    for src_path in matches:
        src_path = Path(src_path)
        try:
            yield src_path.name, src_path.read_bytes()
        except OSError as e:
            logger.error(f"échec de lecture de {src_path.name} : {e}")


def extract_zip_source(source):
    """Génère (nom_fichier, contenu) pour chaque entrée du zip correspondant à
    internal_pattern, en décompressant celles qui finissent en .gz."""
    try:
        archive = zipfile.ZipFile(source["zip_path"])
    except (OSError, zipfile.BadZipFile) as e:
        logger.error(f"impossible d'ouvrir l'archive {source['zip_path']} : {e}")
        return

    with archive:
        matches = [n for n in archive.namelist() if fnmatch.fnmatch(n, source["internal_pattern"])]
        if not matches:
            logger.warning(f"aucune entrée ne correspond au motif : {source['internal_pattern']}")
            return

        for entry_name in matches:
            entry_filename = Path(entry_name).name
            try:
                raw = archive.read(entry_name)
                if entry_filename.lower().endswith(".gz"):
                    yield entry_filename[:-3], gzip.decompress(raw)  # retire le .gz
                else:
                    yield entry_filename, raw
            except (zipfile.BadZipFile, gzip.BadGzipFile, OSError, EOFError) as e:
                logger.error(f"échec d'extraction de {entry_name} : {e}")


def infer_extension(response, url):
    """Déduit l'extension du fichier à partir du Content-Type, ou de l'URL en repli."""
    content_type = response.headers.get("Content-Type", "").split(";")[0].strip()
    ext = mimetypes.guess_extension(content_type) if content_type else None

    if not ext:
        url_ext = Path(urlparse(url).path).suffix
        ext = url_ext if url_ext else ".bin"

    return ext


def extract_api_source(source):
    """Génère (nom_fichier, contenu) pour la réponse d'un point d'accès HTTP GET."""
    headers = {}
    auth_env_var = source.get("auth_env_var")
    if auth_env_var:
        token = os.environ.get(auth_env_var)
        if not token:
            logger.warning(f"ignoré : variable d'environnement {auth_env_var} non définie")
            return
        headers["Authorization"] = f"Bearer {token}"

    try:
        response = requests.get(source["url"], headers=headers, params=source.get("params"))
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"échec de la requête API vers {source['url']} : {e}")
        return

    ext = infer_extension(response, source["url"])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    yield f"{source['name']}_{timestamp}{ext}", response.content


def extract_all():
    """Parcourt toutes les sources déclarées et génère (nom_source, nom_fichier, contenu),
    entièrement en mémoire."""
    for source in SOURCES:
        name = source["name"]
        logger.info(f"[{name}] ({source['type']})")

        if source["type"] == "path":
            entries = extract_path_source(source)
        elif source["type"] == "zip":
            entries = extract_zip_source(source)
        elif source["type"] == "api":
            entries = extract_api_source(source)
        else:
            logger.error(f"type de source inconnu : {source['type']}")
            continue

        for filename, content in entries:
            logger.info(f"extrait en mémoire : {filename} ({len(content):,} octets)")
            yield name, filename, content


def main():
    """Lance l'extraction et affiche un résumé (rien n'est écrit sur disque)."""
    total_files = 0
    total_bytes = 0
    for _name, _filename, content in extract_all():
        total_files += 1
        total_bytes += len(content)
    logger.info(f"Total : {total_files} fichiers, {total_bytes:,} octets récupérés en mémoire.")


if __name__ == "__main__":
    main()
