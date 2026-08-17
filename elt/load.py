"""Pipeline ELT - Phase 2 : Chargement (Load, cluster distribué).

Déclenche l'exécution distribuée du RDD construit par extract.extract_all()
: chaque exécuteur écrit directement dans Data Lake/<nom_source>/<nom_fichier>
les fichiers qu'il a lui-même traités, sans jamais rapatrier les octets
vers le driver (foreachPartition). C'est ici que les données touchent le
disque pour la première fois — et c'est cette action qui déclenche
réellement le calcul distribué défini par extract_all() (Spark est
paresseux : sans action, rien ne s'exécute).

Non destructif : n'écrase un fichier existant que si son contenu a changé
(comparaison par hash). Relancer le script est donc sans risque.

Une erreur d'écriture sur un fichier est loggée et n'interrompt pas le
chargement des autres — voir logs/pipeline.log (écrit par chaque
exécuteur, dans le volume partagé).

Doit être exécuté sur le cluster Spark (voir vm cluster/docker-compose.yml).
Utilisation (depuis le conteneur spark-submit) :
    spark-submit --master spark://spark-master:7077 load.py
"""
import hashlib
from pathlib import Path

from extract import extract_all, get_spark
from logging_config import get_logger

DATA_LAKE = Path(__file__).parent.parent / "Data Lake"


def bytes_hash(data):
    """Calcule le hash de données en mémoire pour détecter les changements."""
    return hashlib.sha256(data).hexdigest()


def write_if_changed(source_name, filename, content):
    """Écrit `content` dans Data Lake/<source_name>/<filename>, sauf si le fichier
    existant est déjà identique. Exécuté sur l'exécuteur, pas sur le driver."""
    logger = get_logger("load")
    dest_dir = DATA_LAKE / source_name
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename
    try:
        if dest_path.exists() and bytes_hash(dest_path.read_bytes()) == bytes_hash(content):
            logger.info(f"inchangé, ignoré : {filename}")
            return
        dest_path.write_bytes(content)
        logger.info(f"chargé : {filename}")
    except OSError as e:
        logger.error(f"échec d'écriture de {filename} : {e}")


def load_partition(rows):
    """Écrit chaque ligne d'une partition du RDD — s'exécute sur l'exécuteur."""
    for source_name, filename, content in rows:
        write_if_changed(source_name, filename, content)


def main():
    """Charge dans le Data Lake, en parallèle sur le cluster, toutes les données extraites."""
    logger = get_logger("load")
    DATA_LAKE.mkdir(exist_ok=True)
    logger.info(f"Data Lake : {DATA_LAKE}")

    spark = get_spark()
    rdd = extract_all(spark)
    rdd.foreachPartition(load_partition)
    spark.stop()


if __name__ == "__main__":
    main()
