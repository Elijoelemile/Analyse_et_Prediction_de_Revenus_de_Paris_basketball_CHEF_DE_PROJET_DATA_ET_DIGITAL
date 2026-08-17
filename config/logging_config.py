"""Configuration du logging partagée par tous les scripts du pipeline.

Chaque script appelle get_logger(nom) pour obtenir un logger qui écrit à
la fois dans la console et dans un fichier de log commun
(logs/pipeline.log, à la racine du projet), avec horodatage, niveau de
gravité et nom du script d'origine.

Important pour l'exécution distribuée (cluster Spark) : get_logger() doit
être appelée À L'INTÉRIEUR de toute fonction susceptible de tourner sur un
executor (pas une seule fois au niveau module), car un logger déjà
configuré sur le driver ne survit pas correctement au transfert vers un
autre processus (cloudpickle). Chaque processus doit reconfigurer son
propre logger localement — get_logger() le fait sans dégâts si on
l'appelle plusieurs fois (elle ne réajoute pas de handlers en double).

Le chemin du fichier de log peut être fixé via la variable d'environnement
PBB_LOG_DIR (utilisée dans les conteneurs, où le chemin relatif au fichier
.py ne fonctionne plus si ce fichier a été envoyé sur le cluster via
--py-files, qui le copie dans un répertoire temporaire).
"""
import logging
import os
from pathlib import Path

LOG_DIR = Path(os.environ.get("PBB_LOG_DIR") or (Path(__file__).parent.parent / "logs"))
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
