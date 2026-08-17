"""Configuration du logging partagée par tous les scripts du pipeline.

Chaque script appelle get_logger(__name__) pour obtenir un logger qui
écrit à la fois dans la console et dans un fichier de log commun
(logs/pipeline.log, à la racine du projet), avec horodatage, niveau de
gravité et nom du script d'origine — pratique pour relire l'historique
des erreurs d'un run après coup, script par script ou dans l'ordre
chronologique global.
"""
import logging
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

_FORMATTER = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


def get_logger(name):
    """Retourne un logger configuré (console + fichier partagé) pour le module `name`."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # déjà configuré : évite d'ajouter des handlers en double

    LOG_DIR.mkdir(exist_ok=True)
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(_FORMATTER)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(_FORMATTER)
    logger.addHandler(console_handler)

    return logger
