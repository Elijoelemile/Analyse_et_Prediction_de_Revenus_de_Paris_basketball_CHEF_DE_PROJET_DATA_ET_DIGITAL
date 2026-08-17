"""Pipeline ELT - Phase 2 : Chargement (Load).

Récupère les données en mémoire produites par extract.extract_all() et les
dépose telles quelles dans Data Lake/<nom_source>/<nom_fichier> — c'est ici
que les données brutes touchent le disque pour la première fois dans ce
pipeline (l'extraction, elle, ne persiste rien).

Non destructif : n'écrase un fichier existant que si son contenu a changé
(comparaison par hash). Relancer le script est donc sans risque — les
fichiers inchangés sont ignorés.

Une erreur d'écriture sur un fichier est loggée et n'interrompt pas le
chargement des autres fichiers — voir logs/pipeline.log.

Utilisation :
    python load.py
"""
import hashlib
from pathlib import Path

from extract import extract_all
from logging_config import get_logger

logger = get_logger("load")

DATA_LAKE = Path(__file__).parent.parent / "Data Lake"


def bytes_hash(data):
    """Calcule le hash de données en mémoire pour détecter les changements."""
    return hashlib.sha256(data).hexdigest()


def write_if_changed(dest_path, content):
    """Écrit `content` dans dest_path, sauf si le fichier existant est déjà identique."""
    try:
        if dest_path.exists() and bytes_hash(dest_path.read_bytes()) == bytes_hash(content):
            logger.info(f"inchangé, ignoré : {dest_path.name}")
            return
        dest_path.write_bytes(content)
        logger.info(f"chargé : {dest_path.name}")
    except OSError as e:
        logger.error(f"échec d'écriture de {dest_path.name} : {e}")


def main():
    """Charge dans le Data Lake toutes les données extraites en mémoire."""
    DATA_LAKE.mkdir(exist_ok=True)
    logger.info(f"Data Lake : {DATA_LAKE}")

    for source_name, filename, content in extract_all():
        dest_dir = DATA_LAKE / source_name
        dest_dir.mkdir(exist_ok=True)
        write_if_changed(dest_dir / filename, content)


if __name__ == "__main__":
    main()
